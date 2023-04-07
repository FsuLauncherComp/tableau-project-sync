import json
import tableauserverclient as TSC
import yaml
from typing import Any

def make_project_hierarchy(projects: list, scoped_project_id: str | None = None) -> list:
    # Create a dictionary of nodes, indexed by project ID.
    nodes: dict[str, dict[str, Any]] = {}
    for i in projects:
        id, name, parent_id = i
        nodes[id] = {"id": id, "name": name}

    forest = []
    for i in projects:
        # Get the ID, name, and parent ID for each project.
        id, name, parent_id = i
        node = nodes[id]

        # If we're scoping to a project, only include nodes that are the
        # scoped project or its children.
        if scoped_project_id and id == scoped_project_id:
            forest.append(node)
        # Otherwise, include nodes that don't have a parent ID.
        elif not scoped_project_id and not parent_id:
            forest.append(node)
        else:
            # Get the parent node.
            parent = nodes.get(parent_id, {})
            # If the parent doesn't have any children, create a list for them.
            if not "children" in parent:
                parent["children"] = []
            children = parent["children"]
            children.append(node)

    return forest


def get_projects(server: TSC.Server) -> list[list[str]]:
    # Get all projects on the server
    projects = TSC.Pager(server.projects)
    return [[p.id, p.name, p.parent_id] for p in projects]


def create_project(server: TSC.Server, project_item: TSC.ProjectItem, samples: bool = False) -> TSC.ProjectItem:
    # Create a new project with the specified name and parent ID (if any) on target server
    try:
        project_item = server.projects.create(project_item, samples)
        print("Created a new project called: %s" % project_item.name)
        return project_item
    except TSC.ServerResponseError:
        print("We have already created this project: %s" % project_item.name)
    project_items = server.projects.filter(name=project_item.name)
    return project_items[0]


def project_iterator(server: TSC.Server, projects: list[dict], parent_id: str | None = None) -> None:
    # Iterate through the projects and create them on the target server
    for p in projects:
        top_level_project = TSC.ProjectItem(name=p["name"], parent_id=parent_id)
        top_level_project = create_project(server, top_level_project)
        if p.get("children"):
            project_iterator(server, p["children"], top_level_project.id)


def get_source_server_projects(configs: dict) -> dict:
    # Get all projects from the source server
    source_configs: dict = configs["source_server"]
    file_configs: dict = configs["files"]
    source_tableau_auth: TSC.PersonalAccessTokenAuth = TSC.PersonalAccessTokenAuth(
        source_configs["pat_name"],
        source_configs["pat_value"],
        site_id=source_configs["site_name"],
    )
    source_server: TSC.Server = TSC.Server(source_configs["server_url"])
    source_server.add_http_options({"verify": False})
    source_server.version = source_configs["version"]
    # Sign in to the source server
    with source_server.auth.sign_in_with_personal_access_token(source_tableau_auth):
        with open(file_configs["source_projects"], "w") as sp:
            all_projects: dict = get_projects(source_server)
            json.dump(all_projects, sp)

    return all_projects


def clean_source_server_projects(configs: dict) -> dict:
    # Clean the source server projects
    source_projects_file = configs["files"]["source_projects"]
    cleaned_projects_file = configs["files"]["cleaned_projects"]
    with open(source_projects_file) as lp:
        data = json.load(lp)
        cleaned_projects = make_project_hierarchy(
            data, configs['source_server']['top_level_project_id']
        )

    with open(cleaned_projects_file, "w") as cp:
        json.dump(cleaned_projects, cp)

    return cleaned_projects

def add_projects_to_target_server(configs: dict[str, Any]) -> bool:
    # Add the projects to the target server
    target_configs = configs.get("target_server")
    file_configs = configs.get("files")
    target_tableau_auth = TSC.PersonalAccessTokenAuth(
        target_configs["pat_name"],
        target_configs["pat_value"],
        site_id=target_configs["site_name"],
    )
    target_server = TSC.Server(target_configs["server_url"])
    target_server.add_http_options({"verify": False})
    target_server.version = target_configs["version"]
    with target_server.auth.sign_in_with_personal_access_token(target_tableau_auth):
        with open(file_configs["cleaned_projects"]) as fp:
            data = json.load(fp)
            project_iterator(target_server, data)

    return True


if __name__ == "__main__":
    with open("configs.yml", "r") as file:
        configs = yaml.safe_load(file)

    source_projects = get_source_server_projects(configs)

    cleaned_projects = clean_source_server_projects(configs)

    projects_sync_to_target = add_projects_to_target_server(configs)
