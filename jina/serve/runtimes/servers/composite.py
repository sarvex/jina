import asyncio
import copy
from typing import Any, List

from jina.serve.runtimes.servers import BaseServer


class CompositeServer(BaseServer):
    """Composite Server implementation"""

    def __init__(
            self,
            **kwargs,
    ):
        """Initialize the gateway
        :param kwargs: keyword args
        """
        super().__init__(**kwargs)

        from jina.parsers.helper import _get_gateway_class

        self.servers: List[BaseServer] = []
        for port, protocol in zip(self.ports, self.protocols):
            server_cls = _get_gateway_class(protocol)
            # ignore monitoring and tracing args since they are not copyable
            ignored_attrs = [
                'metrics_registry',
                'tracer_provider',
                'grpc_tracing_server_interceptors',
                'aio_tracing_client_interceptors',
                'tracing_client_interceptor',
            ]
            runtime_args = self._deepcopy_with_ignore_attrs(
                self.runtime_args, ignored_attrs
            )
            runtime_args.port = [port]
            runtime_args.protocol = [protocol]
            server_kwargs = {k: v for k, v in kwargs.items() if k != 'runtime_args'}
            server_kwargs['runtime_args'] = dict(vars(runtime_args))
            server_kwargs['req_handler'] = self._request_handler
            server = server_cls(**server_kwargs)
            self.servers.append(server)

    async def setup_server(self):
        """
        setup GRPC server
        """
        tasks = [asyncio.create_task(server.setup_server()) for server in self.servers]
        await asyncio.gather(*tasks)

    async def shutdown(self):
        """Free other resources allocated with the server, e.g, gateway object, ..."""
        await super().shutdown()
        shutdown_tasks = [
            asyncio.create_task(server.shutdown()) for server in self.servers
        ]
        await asyncio.gather(*shutdown_tasks)

    async def run_server(self):
        """Run GRPC server forever"""
        run_server_tasks = [
            asyncio.create_task(server.run_server()) for server in self.servers
        ]
        await asyncio.gather(*run_server_tasks)

    @staticmethod
    def _deepcopy_with_ignore_attrs(obj: Any, ignore_attrs: List[str]) -> Any:
        """Deep copy an object and ignore some attributes

        :param obj: the object to copy
        :param ignore_attrs: the attributes to ignore
        :return: the copied object
        """

        memo = {id(getattr(obj, k)): None for k in ignore_attrs if hasattr(obj, k)}
        return copy.deepcopy(obj, memo)

    @property
    def _should_exit(self) -> bool:
        should_exit_values = [
            getattr(server.server, 'should_exit', True) for server in self.servers
        ]
        return all(should_exit_values)
