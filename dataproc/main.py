from cluster import create_cluster


def main():
    project_id = 'symbolic-button-438810-n5'
    region = 'us-central1'
    cluster_name = 'uber-spark'
    create_cluster(project_id=project_id, region=region, cluster_name=cluster_name)


if __name__ == "__main__":
    main()