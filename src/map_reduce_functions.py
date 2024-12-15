def calculate_gpa(record) -> tuple[str, str, float]:
    key, grade_data = record
    studentID, term = key
    total_credit_hours: float = 0.0
    total_grade_points: float = 0.0
    grade_point_map: dict[str, int] = {
        "A": 4,
        "B": 3,
        "C": 2,
        "D": 1,
        "F": 0,
    }
    for grade_datum in grade_data:
        total_credit_hours += grade_datum[1]
        total_grade_points += grade_point_map[grade_datum[2]] * grade_datum[1]
    gpa: float = (
        total_grade_points / total_credit_hours
        if total_credit_hours != 0
        else 0
    )
    return studentID, term, gpa


def map_by_semester(record):
    return (
        (record[0], record[1]),  # Key (StudentID, Semester)
        (record[2], record[3], record[4]),  # Value (Class, Hours, Grade)
    )

def map_by_studentid(record):
    return (
        record[0],  # Key (StudentID)
        (record[1], record[2]),  # Value (Semester, GPA)
    )


def reduce_by_studentid(record):
    studentID, grade_data = record
    result = {"StudentID": studentID, "Grades": {}}
    for semester_data in grade_data:
        result["Grades"][semester_data[0]] = semester_data[1]
    return result
