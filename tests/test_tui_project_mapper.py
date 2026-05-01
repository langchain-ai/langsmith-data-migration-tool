"""Unit tests for the project mapping TUI model."""

from langsmith_migrator.cli.tui_project_mapper import ProjectMapperApp


def test_project_mapper_keeps_duplicate_source_project_ids_visible():
    """Duplicate same-name source projects should remain separate mapping rows."""

    app = ProjectMapperApp(
        [
            {
                "id": "a3cee8e2-40bb-472e-8456-c660b5ea1f3d",
                "name": "Shared Project",
                "tenant_id": "src-ws",
            },
            {
                "id": "source-other-project",
                "name": "Shared Project",
                "tenant_id": "src-ws",
            },
            {
                "id": "source-third-project",
                "name": "Other Project",
                "tenant_id": "src-ws",
            },
        ],
        [
            {
                "id": "cc3ac580-destination-project",
                "name": "Shared Project",
                "tenant_id": "dst-ws",
            }
        ],
    )

    assert len(app.mappings) == 3
    assert {mapping.source_id for mapping in app.mappings} == {
        "a3cee8e2-40bb-472e-8456-c660b5ea1f3d",
        "source-other-project",
        "source-third-project",
    }
    assert any("a3cee8e2" in mapping.source_label for mapping in app.mappings)
