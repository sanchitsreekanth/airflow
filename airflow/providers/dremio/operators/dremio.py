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

from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from airflow.configuration import conf
from airflow.models import BaseOperator
from airflow.providers.dremio.hooks.dremio import DremioException, DremioHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DremioCreateReflectionOperator(BaseOperator):
    """
    Creates a reflection for a Dremio source if it does not exist or updates the reflection if it already exists.

    . seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DremioCreateReflectionOperator`
    """

    ui_color = "#34B8C8"
    template_fields: Sequence[str] = (
        "reflection_spec",
        "source",
        "sql",
    )
    template_fields_renderers = {"reflection_spec": "json"}

    def __init__(
        self,
        source: str,
        reflection_spec: dict[str, Any],
        dremio_conn_id: str = "dremio_default",
        auto_inference: bool = False,
        wait_for_completion: bool = False,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
        sql: str | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        self.source = source
        self.reflection_spec = reflection_spec
        self.dremio_conn_id = dremio_conn_id
        self.auto_inference = auto_inference
        self.deferrable = deferrable
        self.wait_for_completion = (wait_for_completion,)
        self.timeout = timeout
        self.check_interval = check_interval
        self.sql = sql
        super().__init__(**kwargs)

    @cached_property
    def hook(self):
        return DremioHook(dremio_conn_id=self.dremio_conn_id)

    def execute(self, context: Context) -> Any:
        source_spec = self.hook.get_catalog_by_path(self.__get_path(self.source))
        if source_spec.get("type") == "PHYSICAL_DATASET":
            self.hook.refresh_table_metadata(table=self.source)
        elif source_spec.get("type") == "VIRTUAL_DATASET" and not self.sql:
            raise DremioException(f"Virtual dataset {self.source} requires 'sql' parameter to be defined")

        existing_reflections = self.hook.get_reflections_for_source(self.source)
        self.reflection_spec = self.create_reflection_spec(source_spec)
        if self.reflection_spec.get("name") in [
            reflection.get("name") for reflection in existing_reflections
        ]:
            self.log.info(
                "Reflection %s already exists for %s", self.reflection_spec.get("name"), self.source
            )

        else:
            self.log.info("Going to create reflection %s for ", self.reflection_spec.get("name"), self.source)
            self.hook.run(method="POST", endpoint="reflection", body=self.reflection_spec)

    def __get_path(self, source_name: str):
        return source_name.replace(".", "/")

    def create_reflection_spec(self, source_spec: dict):
        spec = {"datasetId": source_spec.get("id"), "entityType": "reflection"}
        if self.reflection_spec.get("displayFields"):
            self.log.warning(
                "Going to skip auto infer display fields since 'reflection_spec' provided already contains displayFields"
            )
        else:
            self.log.info("Going to infer displayFields since 'auto_inference' is true")
            source_fields = [field.get("name") for field in source_spec.get("fields", [])]
            spec["displayFields"] = [{"name": name} for name in source_fields]

        return {**self.reflection_spec, **spec}
