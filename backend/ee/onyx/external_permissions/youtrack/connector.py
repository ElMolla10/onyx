"""The permission-aware side of the YouTrack connector: the walk that names
who may read each issue, on top of the Community Edition connector."""

from ee.onyx.external_permissions.youtrack.access import (
    instance_key,
    issue_access,
    project_access,
)
from onyx.configs.constants import DocumentSource
from onyx.connectors.credentials_provider import build_db_credentials_provider
from onyx.connectors.factory import build_connector_kwargs
from onyx.connectors.interfaces import (
    GenerateSlimDocumentOutput,
    SecondsSinceUnixEpoch,
    SlimConnectorWithPermSync,
)
from onyx.connectors.models import HierarchyNode, SlimDocument
from onyx.connectors.youtrack.connector import YouTrackConnector
from onyx.db.models import ConnectorCredentialPair
from onyx.indexing.indexing_heartbeat import IndexingHeartbeatInterface


def _beat(callback: IndexingHeartbeatInterface | None, amount: int) -> None:
    """Renews the sync's lease and honors a stop between pages."""
    if callback is None:
        return
    if callback.should_stop():
        raise RuntimeError("YouTrack permission sync stopped on the heartbeat's signal")
    callback.progress("youtrack_perm_sync", amount)


class YouTrackPermSyncConnector(YouTrackConnector, SlimConnectorWithPermSync):
    def retrieve_all_slim_docs_perm_sync(
        self,
        start: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        end: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        callback: IndexingHeartbeatInterface | None = None,
    ) -> GenerateSlimDocumentOutput:
        """Every issue in scope with who may read it. The doc sync makes any
        indexed issue this does not list private, so the window is ignored."""
        instance: str = instance_key(self.base_url)
        seen_projects: set[str] = set()
        query: str | None = self.scoped_query()
        if query is None:
            return
        for page in self.ops.iterate_issue_access(query=query):
            batch: list[SlimDocument | HierarchyNode] = []
            for issue in page:
                if issue.project.id not in seen_projects:
                    seen_projects.add(issue.project.id)
                    node: HierarchyNode = self._project_node(issue.project)
                    node.external_access = project_access(instance, issue.project.id)
                    batch.append(node)
                batch.append(
                    SlimDocument(
                        id=self.doc_id(issue.id),
                        external_access=issue_access(instance, issue),
                        parent_hierarchy_raw_node_id=issue.project.id,
                    )
                )
            yield batch
            _beat(callback, len(page))


def youtrack_connector(cc_pair: ConnectorCredentialPair) -> YouTrackPermSyncConnector:
    connector = YouTrackPermSyncConnector(
        **build_connector_kwargs(
            cc_pair.connector.source, cc_pair.connector.connector_specific_config
        )
    )
    connector.set_credentials_provider(
        build_db_credentials_provider(DocumentSource.YOUTRACK, cc_pair.credential.id)
    )
    return connector
