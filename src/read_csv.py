import csv
import os


def get_input_from_csv_dir(
    csv_dir_path: str,
) -> list[tuple[str, str, str, int, str]]:
    records = []
    for csv_file_path in os.listdir(csv_dir_path):
        csv_file_path = os.path.join(csv_dir_path, csv_file_path)
        if not csv_file_path.endswith(".csv") or not os.path.isfile(
            csv_file_path
        ):
            continue
        with open(csv_file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                records.append(
                    (
                        row["StudentID"],
                        row["Semester"],
                        row["Course"],
                        int(row["Hours"]),
                        row["Grade"].strip(),
                    )
                )
    return records
