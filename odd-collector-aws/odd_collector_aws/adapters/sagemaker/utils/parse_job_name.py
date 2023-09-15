import re


def parse_job_name(name: str) -> str:
    """
    Helper function to get name of trial component (job)

    @param: name - sagemaker processing or training job name
    @rtype: object
    """
    parsed = re.search(
        r"^([a-z]*-[a-z|0-9]*-+)(.*)(-.*-aws-(training|processing)-job)$", name
    )

    return parsed.group(2) if parsed else name
