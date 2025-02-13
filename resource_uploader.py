import os
from ..parser.models import ResourceDto
from typing import *
import aiohttp
from cfg import api_url
from dataclasses import asdict
from ..utils.str import bulleted_list
from src.exceptions import ResourceUploadError


class ResourceUploader:
    def __init__(self):
        self.err_log = []

    async def upload(self, resources: List[ResourceDto], verbose: bool) -> int:
        n_uploaded = 0
        err = 0
        uploaded = []

        # Upload each parsed resource
        async with aiohttp.ClientSession() as session:
            for resource in resources:
                async with session.post(
                    api_url + "/resource/save", json=(asdict(resource))
                ) as resp:
                    data = await resp.json()
                    if verbose:
                        print(data)
                    if resp.status == 200:
                        n_uploaded += 1
                        uploaded.append(data["data"]["id"])
                    else:
                        self.err_log.append(
                            f"Resource {resource.businessName} failed to upload: {data}"
                        )
                        err += 1

        outname = f"rollback_ids/{os.getpid()}.txt"

        if len(uploaded) > 0:
            # Create a directory to store the rollback IDs
            if not os.path.exists("rollback_ids"):
                os.mkdir("rollback_ids")

            verbose and print(f"Rollback IDs: {uploaded}")

            # Write the rollback IDs to a file
            with open(outname, "w") as file:
                file.write(" ".join(map(str, uploaded)))

            verbose and print(f"Rollback IDs written to {outname}")

        if err > 0:
            raise ResourceUploadError(
                f"Failed to upload {err} resources:\n{bulleted_list(self.err_log)}"
            )

        return n_uploaded
