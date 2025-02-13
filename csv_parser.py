import sys
import os
import asyncio
import csv
from typing import *
from ..utils.http import all_categories
from ..utils.str import bulleted_list
from ..utils.parsing import (
    validate_field,
    validate_enum_field,
    validate_list_enum_field,
    parse_grade,
)
from .models import ResourceDto
from cfg import allowed_fields
from dataclasses import asdict
from .enums import (
    State,
    Cost,
    Interest,
    InstructionType,
    BoardingType,
    LocationLimit,
    Gender,
)


class CSVParser:

    def __init__(self):
        # Files
        self.files = set()

        # Parsed resources
        self.resources = []

        # Flags
        self.verbose_flag = False
        self.post_flag = False
        self.rollback_flag = False

        # Categories
        self.categories = []
        self.subcategories = []

        # State
        self.err_log = []

    async def parse(self):
        # Initialize files and flags
        self._process_args()

        # Populate category lists
        self.categories, self.subcategories = await all_categories()
        self.categories, self.subcategories = set(self.categories), set(
            self.subcategories
        )

        # Parse the files
        n_rows = await self._parse_all()
        print(f"Successfully parsed {n_rows} rows from {len(self.files)} files")

    async def _parse_all(self):
        tasks = []
        for fp in self.files:
            tasks.append(self._parse_csv(fp))

        return sum(await asyncio.gather(*tasks))

    async def _parse_csv(self, fp: str) -> int:
        n_rows = 0

        with open(fp, "r", newline="", encoding="utf8") as file:
            # Open the reader
            reader = csv.DictReader(file)

            # Validate the field names
            self._validate_column_names(fp, reader.fieldnames)

            # Iterate through each row
            for row in reader:
                res = await self._parse_row(row)
                self.resources.append(res)
                n_rows += 1
                self.verbose_flag and print(f"Parsed {asdict(res)}")

        return n_rows

    async def _parse_row(self, row: dict[str | Any, str | Any]) -> ResourceDto:
        keys = row.keys()

        resource = ResourceDto(
            id=0,
            businessName=None,
            streetAddress=None,
            state=None,
            city=None,
            zipCode=None,
            phone=None,
            webSite=None,
            description=None,
        )

        # Check that required fields exist
        self._validate_required_fields(row)

        resource.businessName = row["businessName"]
        resource.id = 0

        # Validate and apply simple fields TODO: get rid of validate_field
        for field in [
            "streetAddress",
            "city",
            "zipCode",
            "phone",
            "webSite",
            "description",
        ]:
            if res := validate_field(field, row):
                setattr(resource, field, row[field])

        # Validate and apply category/subcategory
        resource.category = self._validate_category_field(
            row["category"], subcategory=False
        )

        resource.subCategory = self._validate_category_field(
            row["subCategory"], subcategory=True
        )

        # Validate and apply start/end grade
        if "GRADELEVEL" in keys and (res := parse_grade(row["GRADELEVEL"])):
            resource.startGrade, resource.endGrade = res

        # Validate and apply state
        if res := validate_enum_field(State, "state", row):
            resource.state = res

        # Validate and apply career interest
        if res := validate_list_enum_field(Interest, "careerInterest", row):
            resource.careerInterest = res

        # Validate and apply cost
        if res := validate_enum_field(Cost, "cost", row):
            resource.cost = res

        # Validate and apply instruction type
        if res := validate_list_enum_field(InstructionType, "instruction", row):
            resource.instruction = res

        # Validate and apply boarding type
        if res := validate_list_enum_field(BoardingType, "boarding", row):
            resource.boarding = res

        # Validate and apply gender
        if res := validate_enum_field(Gender, "gender", row):
            resource.gender = res

        # Validate and apply location limit
        if res := validate_list_enum_field(LocationLimit, "locationLimit", row):
            resource.locationLimit = res

        return resource

    def _process_args(self) -> None:
        # Program needs arguments
        if len(sys.argv) < 2:
            raise Exception(
                "Program expects at least 1 argument.\n\nUsage: python <path to main.py> <directories containing CSV files | CSV names delimited by spaces>"
            )

        n_failed = 0
        for i in range(1, len(sys.argv)):
            s = sys.argv[i]

            # Check flags
            if s.startswith("-"):
                self._set_flag(s)
                continue

            # Populate self.files with valid files
            n_failed += self._process_fp(s)

        # Check if any files do not exist
        if n_failed > 0:
            raise FileNotFoundError(
                f"Some paths were not found:\n{bulleted_list(self.err_log)}"
            )

        if self.verbose_flag:
            print(f"CSV files stored:\n{bulleted_list(self.files)}\n")

    def _set_flag(self, flag: str) -> None:
        match (flag.lower()):
            case "-v" | "--verbose":
                self.verbose_flag = True
            case "--upload":
                self.post_flag = True
            case "-r" | "--rollback":
                self.rollback_flag = True

    def _process_fp(self, fp: str) -> int:
        # Base case: fp is a file
        if fp.endswith(".csv") and os.path.isfile(fp):
            self.files.add(fp)
            return 0

        # Recursive case: fp is a directory
        if os.path.isdir(fp):
            nfiles = 0
            for p in os.listdir(fp):
                nfiles += self._process_fp(f"{fp}/{p}")

            return nfiles

        # File exists, but isn't a CSV
        if os.path.exists(fp):
            return 0

        self.err_log.append(f'Path "{fp}" does not exist')
        return 1

    def _validate_column_names(self, fp: str, fields: Iterable[str]) -> None:
        attrs = ResourceDto.__annotations__.keys()

        err = False
        for field in fields:
            if field not in attrs and field not in allowed_fields:
                self.err_log.append(f'{fp}: "{field}" is not a valid field')
                err = True

        if err:
            raise Exception(
                f"Unable to parse {fp} due to invalid fields:\n{bulleted_list(self.err_log)}"
            )

    def _validate_required_fields(self, row: dict[Any | str, Any | str]) -> None:
        err = False
        for field in ["businessName", "category", "subCategory"]:
            if field not in row:
                err = True
                self.err_log.append(f'"{field}" is a required row')

        if err:
            raise Exception(
                f"At least 1 required field was not found in one of the rows:\n{bulleted_list(self.err_log)}"
            )

    def _validate_category_field(self, cats: str, subcategory=False) -> str:
        cats = cats.split("+")
        for cat in cats:
            if cat not in (self.subcategories if subcategory else self.categories):
                raise Exception(
                    f'"{cat}" is not a valid {"sub" if subcategory else ""}category'
                )
        return cats
