from django.http import HttpResponse, JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from pathlib import Path
import json

_BACKEND_DIR = Path(__file__).resolve().parent.parent
_DEMO_COMM_DIR = _BACKEND_DIR.parent / "demo" 
_TABLES_DIR = _DEMO_COMM_DIR / "tables"
_JOB_RESULT_PATH = _DEMO_COMM_DIR / "response"
_DT_DIR = _DEMO_COMM_DIR / "decision_trees"


def read_json_file(path: Path):
    """Read JSON from path. Returns (data, None) on success or (None, error_msg)."""
    if not path.is_file():
        return None, f"File not found: {path}"
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f), None
    except json.JSONDecodeError as e:
        return None, f"JSON parse error: {e}"


def hello_world(request):
    return HttpResponse("Hello, World!")


@csrf_exempt
@require_http_methods(["POST"])
def get_table_data(request):
    table_name = request.POST.get("table")
    if not table_name:
        return JsonResponse(
            {"status": "error", "message": "Table name is required"}, status=400
        )

    path = _TABLES_DIR / f"{table_name}.json"
    data, err = read_json_file(path)
    if err:
        return JsonResponse({"status": "error", "message": err}, status=503)
    return JsonResponse(data)


@csrf_exempt
@require_http_methods(["POST"])
def job_join(request):
    method_raw = request.POST.get("method")
    q_name = request.POST.get("q_name")
    sql_text = request.POST.get("sql")
    if method_raw is None or q_name is None or sql_text is None:
        return JsonResponse(
            {"status": "error", "message": "method, q_name and sql are required"},
            status=400,
        )
    try:
        method_id = int(method_raw)
    except ValueError:
        return JsonResponse(
            {"status": "error", "message": "method must be an integer"}, status=400
        )
    if method_id < 0 or method_id > 31:
        return JsonResponse(
            {"status": "error", "message": "method out of supported range"}, status=400
        )
    if not q_name.strip():
        return JsonResponse(
            {"status": "error", "message": "q_name must be non-empty"}, status=400
        )
    if not sql_text.strip():
        return JsonResponse(
            {"status": "error", "message": "sql must be non-empty"}, status=400
        )

    safe_name = Path(q_name).name
    path = _JOB_RESULT_PATH / f"{method_id}" / f"{safe_name}.json"
    data, err = read_json_file(path)
    if err:
        return JsonResponse({"status": "error", "message": err}, status=503)
    return JsonResponse(data)


@require_http_methods(["GET"])
def decision_tree_models(request):
    path = _DT_DIR / "models.json"
    data, err = read_json_file(path)
    if err:
        return JsonResponse({"status": "error", "message": err}, status=503)
    return JsonResponse({"models": data})


@csrf_exempt
@require_http_methods(["POST"])
def decision_tree_data(request):
    model_id = request.POST.get("model_id")
    if not model_id:
        return JsonResponse(
            {"status": "error", "message": "model_id is required"}, status=400
        )
    safe_id = Path(model_id).name
    path = _DT_DIR / f"{safe_id}.json"
    data, err = read_json_file(path)
    if err:
        return JsonResponse({"status": "error", "message": err}, status=503)
    return JsonResponse(data)
