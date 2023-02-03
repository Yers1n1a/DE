from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from param_web_to_gcs import etl_parent_flow


# github_block = GitHub.load("github-ny-taxi")

# gh_dep = Deployment.build_from_flow(
#     flow=etl_parent_flow,
#     name="github-ny-taxi",
#     infrastructure=github_block,
# )

gh = GitHub(repository='https://github.com/Yers1n1a/DE.git')
gh.save('prefect-gh-flow', overwrite=True)


# if __name__ == "__main__":
#     gh_dep.apply()
