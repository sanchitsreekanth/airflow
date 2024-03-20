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
from functools import cached_property
from json import JSONDecodeError
from typing import Any

import requests
from requests.models import DEFAULT_REDIRECT_LIMIT, HTTPError

from airflow.hooks.base import BaseHook


class DremioException(Exception):
    """Dremio Exception class."""


def _trim_trailing_slash(url: str) -> str:
    """Remove trailing / from url."""
    if url.endswith("/"):
        return url[:-1]
    return url


def _url_from_endpoint(base_url: str | None, endpoint: str | None) -> str:
    """Combine base url with endpoint."""
    if base_url and not base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
        return f"{base_url}/{endpoint}"
    return (base_url or "") + (endpoint or "")


def _jsonify(obj: Any) -> dict:
    """Return a JSON serializable object from obj."""
    if not obj:
        return {}

    if isinstance(obj, str):
        try:
            json_val = json.loads(obj)
            return json_val
        except JSONDecodeError:
            raise DremioException(f"Invalid json string provided - {obj}")
    elif isinstance(obj, dict):
        return obj
    else:
        return {}


class DremioHook(BaseHook):
    """Interacts with Dremio Cluster using Dremio REST APIs."""

    conn_attr_name = "dremio_conn_id"
    default_conn_name = "dremio_default"
    conn_type = "http"
    hook_name = "Dremio"
    default_api_version = "api/v3"

    def __init__(
        self,
        dremio_conn_id: str = default_conn_name,
        headers: dict | None = None,
        api_version: str = default_api_version,
        *args,
        **kwargs,
    ) -> None:
        self.dremio_conn_id = dremio_conn_id
        self.api_version = api_version
        self.headers = headers or {"Content-Type": "application/json"}
        super().__init__(*args, **kwargs)

    @cached_property
    def url(self) -> str:
        conn = self.get_connection(conn_id=self.dremio_conn_id)
        base_url = ""
        host = _trim_trailing_slash(conn.host) if conn.host else ""
        if "://" in host:
            base_url = base_url + host
        else:
            schema = conn.schema if conn.schema else "http"
            base_url = f"{schema}://{host}"

        if conn.port:
            base_url = f"{base_url}:{conn.port}"

        return base_url

    @cached_property
    def auth_headers(self) -> dict:
        conn = self.get_connection(conn_id=self.dremio_conn_id)
        extra = conn.extra_dejson if conn.extra else {}
        if not extra:
            return {}

        auth = extra.get("auth", None)
        pat = extra.get("pat", None)
        if not auth:
            self.log.info("No authentication provided for Dremio")
            return {}

        if auth and auth not in ("AuthToken", "PAT"):
            raise ValueError(
                f"Received invalid auth type {auth} in connection extra. Please provide either 'AuthToken' or PAT' for authentication. Please refer https://docs.dremio.com/current/reference/api/#authentication for more details"
            )

        if auth == "AuthToken":
            if not all([conn.login, conn.password]):
                raise ValueError(
                    f"Both login and password must be set in {self.dremio_conn_id} connection for using AuthToken"
                )

            token = self.__fetch_auth_token(conn.login, conn.password)
            return {"Authorization": f"_dremio{token}"}

        elif auth == "PAT":
            if conn.extra and not pat:
                raise AttributeError(
                    "Auth method 'PAT' requires a value for key 'pat' in connection extra fields which contains value for the pat token"
                )

            return {"Authorization": f"Bearer {pat}"}

        else:
            return {}

    def __fetch_auth_token(self, username: str, password: str):
        token_url = _url_from_endpoint(base_url=self.url, endpoint="apiv2/login")
        data = json.dumps({"userName": username, "password": password})
        headers = {"Content-Type": "application/json"}
        response = requests.post(url=token_url, data=data, headers=headers)
        self.handle_response(response)
        return response.json().get("token")

    def get_conn(self) -> requests.Session:
        session = requests.Session()
        conn = self.get_connection(conn_id=self.dremio_conn_id)

        host = conn.host if conn.host else ""
        if not host:
            raise DremioException(
                f"The connection {self.dremio_conn_id} does not have the dremio hostname set. "
                f"Please give a valid hostname in the airflow connection"
            )

        if conn.extra:
            extra = conn.extra_dejson
            extra.pop("timeout", None)  # ignore this as timeout is only accepted in request method of Session
            extra.pop("allow_redirects", None)  # ignore this as only max_redirects is accepted in Session
            session.proxies = extra.pop("proxies", extra.pop("proxy", {}))
            session.stream = extra.pop("stream", False)
            session.verify = extra.pop("verify", extra.pop("verify_ssl", True))
            session.cert = extra.pop("cert", None)
            session.max_redirects = extra.pop("max_redirects", DEFAULT_REDIRECT_LIMIT)

            try:
                session.headers.update(extra)
            except TypeError:
                self.log.warning("Connection to %s has invalid extra field.", conn.host)

        if self.headers:
            session.headers.update(self.headers)

        auth_headers = self.auth_headers
        session.headers.update(auth_headers)

        return session

    def handle_response(self, response: requests.Response):
        try:
            response.raise_for_status()
        except HTTPError:
            self.log.error(response.text)
            raise DremioException(
                f"Failed to send HTTP request to Dremio - {str(response.status_code)} : {response.reason}"
            )

    def run(
        self, method: str, endpoint: str, body: dict | str | None = None, params: dict | str | None = None
    ):
        body = _jsonify(body)
        params = _jsonify(params)
        method = method.upper()
        base_url = _url_from_endpoint(base_url=self.url, endpoint=self.api_version)
        url = _url_from_endpoint(base_url=base_url, endpoint=endpoint)
        session = self.get_conn()

        if method == "GET":
            request = requests.Request("GET", url=url, params=params, headers=session.headers)

        else:
            request = requests.Request(method, url=url, data=json.dumps(body), headers=session.headers)

        prepped_request = session.prepare_request(request)
        response = session.send(prepped_request)
        self.handle_response(response)
        if response.text:
            return response.json()

    def execute_sql_query(self, sql: str, **sql_kwargs):
        body = {**{"sql": sql, **sql_kwargs}}
        self.log.info("Executing SQL query %s", sql)
        response = self.run(method="POST", body=body, endpoint="sql")
        return response

    def get_catalog_by_path(self, path: str, **kwargs):
        return self.run(method="GET", params=kwargs, endpoint=f"catalog/by-path/{path}")

    def get_catalog(self, catalog_id: str, **kwargs):
        return self.run(method="GET", params=kwargs, endpoint=f"catalog/{catalog_id}")

    def get_reflection(self, reflection_id: str):
        return self.run(method="GET", endpoint=f"reflection/{reflection_id}")

    def create_reflection(self, reflection_spec: dict):
        self.run(method="POST", endpoint="reflection", body=reflection_spec)

    def update_reflection(self, reflection_id: str, spec: dict):
        self.run(method="PUT", endpoint=f"reflection/{reflection_id}", body=spec)

    def get_dataset_reflection(self, dataset_id: str):
        return self.run(method="GET", endpoint=f"dataset/{dataset_id}/reflection")

    def get_reflections_for_source(self, source: str):
        dataset_id = self.get_catalog_by_path(path=source.replace(".", "/")).get("id")
        response = self.get_dataset_reflection(dataset_id)
        return response.get("data")

    def get_job(self, job_id: str):
        return self.run(method="GET", endpoint=f"job/{job_id}")

    def get_job_results(self, job_id):
        return self.run(method="GET", endpoint=f"job/{job_id}/results")

    def refresh_table_metadata(self, table: str, context: list[str] | None = None):
        sql = f"ALTER TABLE {table} REFRESH METADATA"
        sql_kwargs = {"context": context} if context else {}
        job_id = self.execute_sql_query(sql=sql, **sql_kwargs).get("id")
        self.log.info("Job id for %s metadata refresh is %s", table, job_id)
        return job_id

    def set_property(self, property_name: str, value: str, level: str = "system"):
        return self.execute_sql_query(sql=f"ALTER {level.upper()} SET {property_name} = {value}")

    def unset_property(self, property_name: str, level: str = "system"):
        return self.execute_sql_query(sql=f"ALTER {level.upper()} RESET {property_name}")
