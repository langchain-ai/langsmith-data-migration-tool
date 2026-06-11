"""Tests for the standalone project mapper TUI helpers."""

from __future__ import annotations

from types import SimpleNamespace

from langsmith_migrator.cli.tui_project_mapper import (
    DestinationPickerScreen,
    MappingStatus,
    ProjectMapperApp,
)


def test_destination_picker_enter_uses_only_matching_suggestion_label(monkeypatch):
    """Pressing Enter in the input should select the single visible ID-bearing row."""

    destination_label = (
        "smoke-997-career-advisor-agent (cc3ac580) "
        "· ws:12c99b9c-0753-4c51-a22e-828e24f9e908"
    )
    screen = DestinationPickerScreen(
        "smoke-997-career-advisor-agent (a3cee8e2)",
        [destination_label],
        {},
        initial_value="smoke-997-career-advisor-agent",
    )
    captured: list[str] = []
    monkeypatch.setattr(screen, "dismiss", captured.append)

    screen.on_input_submitted(SimpleNamespace(value="smoke-997-career-advisor-agent"))

    assert captured == [destination_label]


def test_destination_picker_enter_keeps_custom_value_when_no_suggestions(monkeypatch):
    """No-match text remains a custom destination name for name-mapping callers."""

    screen = DestinationPickerScreen(
        "Source Project (src)",
        ["Existing Project (dst)"],
        {},
        initial_value="New Project",
    )
    captured: list[str] = []
    monkeypatch.setattr(screen, "dismiss", captured.append)

    screen.on_input_submitted(SimpleNamespace(value="New Project"))

    assert captured == ["New Project"]


def test_id_mapper_does_not_count_unresolved_text_destination_as_mapped():
    """ID-returning mapper rows need a destination ID before they count as mapped."""

    app = ProjectMapperApp(
        [{"id": "src-project", "name": "Shared Project"}],
        [
            {"id": "dst-project-a", "name": "Shared Project"},
            {"id": "dst-project-b", "name": "Shared Project"},
        ],
        return_ids=True,
    )
    mapping = app.mappings[0]

    app._set_destination(mapping, "Shared Project")

    assert mapping.dest_id is None
    assert mapping.status == MappingStatus.UNMAPPED


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


def test_enter_on_main_table_row_opens_destination_picker(monkeypatch):
    """RowSelected from the main table (Enter key) must trigger action_assign."""

    app = ProjectMapperApp([{"id": "src-1", "name": "Project A"}], [])
    calls = []
    monkeypatch.setattr(app, "action_assign", lambda: calls.append("assign"))

    app.on_data_table_row_selected(SimpleNamespace(data_table=SimpleNamespace(id="main-table")))
    app.on_data_table_row_selected(SimpleNamespace(data_table=SimpleNamespace(id="dest-table")))

    assert calls == ["assign"]
