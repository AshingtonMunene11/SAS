import json
from fastapi.testclient import TestClient
from backend.app import app

client = TestClient(app)

def test_where_keep_rename():
    script = """
    DATA mydata;
    SET data/employees.csv;
    WHERE age > 30;
    KEEP name, age, income;
    RENAME income=salary;
    RUN;
    """
    response = client.post("/run-script", json={"code": script})
    assert response.status_code == 200
    data = response.json()
    assert data["columns"] == ["name", "age", "salary"]
    assert data["shape"] == [2, 3]
    assert all("salary" in row for row in data["preview"])

def test_drop_clause():
    script = """
    DATA mydata;
    SET data/employees.csv;
    DROP gender;
    RUN;
    """
    response = client.post("/run-script", json={"code": script})
    assert response.status_code == 200
    data = response.json()
    assert "gender" not in data["columns"]
    assert data["shape"][1] == 4  # 4 columns after drop

def test_combined_clauses():
    script = """
    DATA mydata;
    SET data/employees.csv;
    WHERE age > 30;
    DROP id, gender;
    RENAME income=salary;
    RUN;
    """
    response = client.post("/run-script", json={"code": script})
    assert response.status_code == 200
    data = response.json()
    assert data["columns"] == ["name", "age", "salary"]
    assert data["shape"] == [2, 3]
def test_multiple_renames():
    script = """
    DATA mydata;
    SET data/employees.csv;
    RENAME name=employee_name, income=salary;
    RUN;
    """
    response = client.post("/run-script", json={"code": script})
    assert response.status_code == 200
    data = response.json()

    assert "employee_name" in data["columns"]
    assert "salary" in data["columns"]

    assert "name" not in data["columns"]
    assert "income" not in data["columns"]

    for row in data["preview"]:
        assert "employee_name" in row
        assert "salary" in row
