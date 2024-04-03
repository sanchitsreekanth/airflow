#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
import time
from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from airflow.configuration import conf
from airflow.models import BaseOperator
from airflow.providers.dremio.hooks.dremio import DremioException, DremioHook
from airflow.providers.dremio.triggers.dremio import ReflectionRefreshTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DremioCreateReflectionOperator(BaseOperator):
    """
    Creates a reflection for a Dremio source if it does not exist or updates if it already exists.

    . seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DremioCreateReflectionOperator`
    """

    ui_color = "#34B8C8"
    template_fields: Sequence[str] = (
        "reflection_spec",
        "source",
        "sql_query",
        "refresh_settings",
    )
    template_fields_renderers = {"reflection_spec": "json", "sql_query": "sql", "refresh_settings": "json"}

    def __init__(
        self,
        source: str,
        reflection_spec: dict[str, Any],
        dremio_conn_id: str = "dremio_default",
        auto_inference: bool = False,
        wait_for_completion: bool = False,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
        sql_query: str | None = None,
        reflection_queue: str | None = None,
        refresh_settings: dict | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        self.source = source
        self.reflection_spec = reflection_spec
        self.dremio_conn_id = dremio_conn_id
        self.auto_inference = auto_inference
        self.deferrable = deferrable
        self.wait_for_completion = wait_for_completion
        self.timeout = timeout
        self.check_interval = check_interval
        self.sql_query = sql_query
        self.reflection_queue = reflection_queue
        self.refresh_settings = refresh_settings
        super().__init__(**kwargs)

    @cached_property
    def hook(self):
        return DremioHook(dremio_conn_id=self.dremio_conn_id)

    @cached_property
    def dataset_spec(self) -> dict:
        return self.hook.get_catalog_by_path(self.__get_path(self.source))

    @cached_property
    def reflection_name(self) -> str | None:
        return self.reflection_spec.get("name")

    @cached_property
    def spec(self) -> dict:
        spec = {"datasetId": self.dataset_spec.get("id"), "entityType": "reflection"}
        if self.auto_inference:
            if self.reflection_spec.get("displayFields"):
                self.log.warning(
                    "'auto_inference' is true and displayFields also provided for 'reflection_spec'. auto inferred fields will override the user provided displayFields",
                    UserWarning,
                )
            self.log.info("Going to infer displayFields since 'auto_inference' is true")
            source_fields = [field.get("name") for field in self.dataset_spec.get("fields", [])]
            spec["displayFields"] = [{"name": name} for name in source_fields]

        return {**self.reflection_spec, **spec}

    def execute(self, context: Context) -> Any:
        self.preprocess()
        reflection_id = self.create_or_update_reflection()
        context["task_instance"].xcom_push(key="reflection_id", value=reflection_id)

        if not self.wait_for_completion:
            self.log.warning("Argument `wait_for_termination` is False. Going to complete task", UserWarning)
            return reflection_id

        if self.deferrable:
            self.execute_async(reflection_id, context)
        else:
            self.execute_sync(reflection_id)

    def __get_path(self, source_name: str):
        return source_name.replace(".", "/")

    def __find_matching_reflection(self) -> dict | None:
        return next(
            (
                ref
                for ref in self.hook.get_reflections_for_source(self.source)
                if ref.get("name") == self.reflection_spec.get("name")
            ),
            None,
        )

    def create_or_update_reflection(self):
        current_spec = self.__find_matching_reflection()

        if current_spec:
            self.log.info("Reflection %s already exists for %s", self.reflection_name, self.source)
            updates = self.reflection_updates(current_spec)
            if updates:
                update_spec = self.get_updated_reflection_body(current_spec, updates)
                self.log.info("Going to update reflection with body %s", json.dumps(update_spec))
                response = self.hook.update_reflection(reflection_id=update_spec.get("id"), spec=update_spec)
            else:
                self.log.info("No updated for reflection. Going to trigger refresh manually")
                response = current_spec
                # TODO: Add reflection refresh setting update if api allows it
                self.hook.trigger_reflection_refresh(self.dataset_spec.get("id"))

        else:
            self.log.info("Going to create reflection %s for ", self.reflection_spec.get("name"), self.source)
            response = self.hook.create_reflection(reflection_spec=self.spec)

        return response.get("id")

    def reflection_updates(self, initial_spec: dict) -> dict:
        # Remove keys not needed for comparison
        keys_to_remove = [
            "id",
            "status",
            "currentSizeBytes",
            "totalSizeBytes",
            "createdAt",
            "updatedAt",
            "entity_type",
            "datasetId",
            "tag",
        ]
        for key in keys_to_remove:
            initial_spec.pop(key, None)

        diff_dict = {}
        for key, new_value in self.spec.items():
            current_value = initial_spec.get(key)
            if isinstance(new_value, list):
                current_value = current_value or []
                current_value = sorted(current_value, key=lambda item: item["name"])
                new_value = sorted(new_value, key=lambda item: item["name"])
            if new_value != current_value:
                diff_dict[key] = new_value

        return diff_dict

    def get_updated_reflection_body(self, initial_spec: dict, updated_spec: dict) -> dict:
        if not updated_spec:
            self.log.info("No updates available for reflection %s", initial_spec.get("name"))
            return {}

        self.log.info("Found updates for reflection - %s", json.dumps(updated_spec))
        # Remove keys not needed for updating reflection
        keys_to_remove = ["status", "currentSizeBytes", "totalSizeBytes", "createdAt", "updatedAt"]
        for key in keys_to_remove:
            initial_spec.pop(key, None)

        return {**initial_spec, **updated_spec}

    def preprocess(self):
        self.run_validations()
        if self.dataset_spec.get("type") == "PHYSICAL_DATASET":
            self.hook.refresh_table_metadata(table=self.source)
        elif self.dataset_spec.get("type") == "VIRTUAL_DATASET" and not self.sql_query:
            raise DremioException(f"Virtual dataset {self.source} requires 'sql' parameter to be defined")

        # Assign queue for reflections if provided
        if self.reflection_queue:
            self.hook.execute_sql_query(
                sql=f"ALTER DATASET {self.dataset_spec.get('id')} QUEUE {self.reflection_queue}"
            )

    def run_validations(self):
        mandatory_keys = ["name", "type"]
        for key in mandatory_keys:
            if key not in self.reflection_spec:
                raise DremioException(f"Key {key} is mandatory but not present in the reflection spec")

        if self.dataset_spec.get("type") == "VIRTUAL_DATASET" and not self.sql_query:
            raise DremioException(f"Virtual dataset {self.source} requires 'sql' parameter to be defined")

        if not self.reflection_spec.get("displayFields") and not self.auto_inference:
            raise DremioException(
                "auto_inference is false and no displayFields provided in reflection_spec which is mandatory for creating reflections"
            )

    def execute_async(self, reflection_id: str, context: Context):
        timeout = time.time() + self.timeout
        self.defer(
            trigger=ReflectionRefreshTrigger(
                conn_id=self.dremio_conn_id,
                reflection_id=reflection_id,
                check_interval=self.check_interval,
                timeout=timeout,
            ),
            method_name="execute_complete",
            timeout=self.execution_timeout,
        )

    def execute_sync(self, reflection_id: str):
        if self.hook.wait_for_reflection_completion(
            reflection_id=reflection_id, check_interval=self.check_interval, timeout=self.timeout
        ):
            self.log.info("Reflection %s has completed successfully", reflection_id)
        else:
            raise DremioException(f"Reflection refresh has failed for reflection id {reflection_id}")

    def execute_complete(self, context: Context, event: dict[str, Any]) -> str:
        status = event.get("status")
        message = event.get("message")
        reflection_id = event.get("reflection_id")
        self.log.info(message)
        if status == "disabled":
            raise DremioException(f"{reflection_id} reflection has been disabled")
        if status == "error":
            raise DremioException(f"Reflection refresh for {reflection_id} has failed")
        return str(reflection_id)
