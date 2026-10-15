"""The groups the doc sync names, per project in scope: the project's readers,
its Override Visibility Restrictions holders, and for every group or user a
restricted issue names, those of its members who are project readers (all of
them in a helpdesk project).

The visibility entries in use come from a walk over the issues, so group
membership tracks role and group changes at each group sync without the doc
sync rewriting any issue. Each group is yielded as soon as the walk first
meets its entry, so the caller's timeout check runs throughout the walk."""

from collections.abc import Generator

from ee.onyx.db.external_perm import ExternalUserGroup
from ee.onyx.external_permissions.youtrack.access import (
    GroupMembers,
    ProjectReaders,
    RoleGrants,
    VisibilityEntries,
    instance_key,
    override_group_id,
    project_group_id,
    user_group_id,
    visibility_entries,
    visibility_group_id,
)
from ee.onyx.external_permissions.youtrack.connector import youtrack_connector
from onyx.connectors.youtrack.connector import YouTrackConnector
from onyx.connectors.youtrack.models import YouTrackUser
from onyx.connectors.youtrack.source_operations import YouTrackSourceOperations
from onyx.db.models import ConnectorCredentialPair


def youtrack_group_sync(
    tenant_id: str,  # noqa: ARG001
    cc_pair: ConnectorCredentialPair,
) -> Generator[ExternalUserGroup, None, None]:
    yield from project_groups(youtrack_connector(cc_pair))


def _member_emails(users: list[YouTrackUser]) -> dict[str, str]:
    """Banned users have no access and the guest account is anonymous, so only
    other users with an email are mapped."""
    return {
        user.id: user.email.strip().lower()
        for user in users
        if user.email and user.email.strip() and not user.banned and not user.guest
    }


def _within(user_ids: set[str], allowed: set[str] | None) -> set[str]:
    return user_ids if allowed is None else user_ids & allowed


def project_groups(
    connector: YouTrackConnector,
) -> Generator[ExternalUserGroup, None, None]:
    ops: YouTrackSourceOperations = connector.ops
    instance: str = instance_key(connector.base_url)
    users: list[YouTrackUser] = ops.list_users()
    emails: dict[str, str] = _member_emails(users)
    # Customers read only what a helpdesk ticket's visibility names, never
    # through a role, even one granted to a group they belong to.
    customers: set[str] = {user.id for user in users if user.is_customer}
    members = GroupMembers(ops)
    grants = RoleGrants(
        ops.list_roles(), ops.list_assigned_roles(), ops.list_organizations()
    )

    def emails_of(user_ids: set[str]) -> list[str]:
        return sorted({emails[user_id] for user_id in user_ids if user_id in emails})

    # Per project, who a visibility entry may grant: everyone in a helpdesk
    # project, whose tickets name every reader, else the project's readers.
    allowed_by_project: dict[str, set[str] | None] = {}
    for project in connector.active_projects():
        access: ProjectReaders = grants.project_readers(project.id, members)
        readers: set[str] = access.readers - customers
        allowed_by_project[project.id] = None if project.is_helpdesk else readers
        # Bare ids: the source prefix is added when the groups are stored.
        yield ExternalUserGroup(
            id=project_group_id(instance, project.id),
            # Never public: guest reads also need a server-wide setting the
            # API does not expose, and every registered reader is listed.
            user_emails=emails_of(readers),
        )
        yield ExternalUserGroup(
            id=override_group_id(instance, project.id),
            user_emails=emails_of(access.overriders - customers),
        )

    query: str | None = connector.scoped_query()
    if query is None:
        return
    emitted: dict[str, VisibilityEntries] = {}
    for page in ops.iterate_issue_access(query=query):
        for issue in page:
            project_id: str = issue.project.id
            named: VisibilityEntries | None = visibility_entries(issue)
            if named is None or project_id not in allowed_by_project:
                continue
            allowed: set[str] | None = allowed_by_project[project_id]
            seen: VisibilityEntries = emitted.setdefault(
                project_id, VisibilityEntries()
            )
            for group_id in named.group_ids - seen.group_ids:
                seen.group_ids.add(group_id)
                yield ExternalUserGroup(
                    id=visibility_group_id(instance, project_id, group_id),
                    user_emails=emails_of(_within(members(group_id), allowed)),
                )
            for user_id in named.user_ids - seen.user_ids:
                seen.user_ids.add(user_id)
                if not _within({user_id}, allowed):
                    continue
                yield ExternalUserGroup(
                    id=user_group_id(instance, project_id, user_id),
                    user_emails=emails_of({user_id}),
                )
