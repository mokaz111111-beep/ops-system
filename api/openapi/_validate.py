"""校验 OpenAPI 文件：YAML 可解析、全部 $ref 可解析、无未引用组件。"""
import sys
import yaml

PATH = "api/openapi/if-007-query-v1.yaml"

with open(PATH, encoding="utf-8") as f:
    doc = yaml.safe_load(f)

refs = []


def walk(node, path=""):
    if isinstance(node, dict):
        for key, val in node.items():
            if key == "$ref" and isinstance(val, str):
                refs.append((val, path))
            else:
                walk(val, f"{path}/{key}")
    elif isinstance(node, list):
        for i, val in enumerate(node):
            walk(val, f"{path}/{i}")


walk(doc)

broken = []
for ref, where in refs:
    if not ref.startswith("#/"):
        broken.append((ref, where, "external"))
        continue
    cur = doc
    for part in ref[2:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            broken.append((ref, where, "unresolved"))
            break

print(f"refs: {len(refs)} total, {len(set(r for r, _ in refs))} unique")

if broken:
    print("BROKEN REFS:")
    for ref, where, why in broken:
        print(f"  [{why}] {ref}  at {where}")
else:
    print("all refs resolve OK")

components = doc["components"]
declared = set()
for group in ("schemas", "parameters", "responses"):
    declared |= {f"{group}:{name}" for name in components.get(group, {})}
used = set()
for ref, _ in refs:
    parts = ref.split("/")
    if len(parts) >= 4 and parts[1] == "components":
        used.add(f"{parts[2]}:{parts[3]}")

unused = sorted(declared - used)
print(f"unused components: {unused if unused else 'none'}")

required_endpoints = [
    "/dimensions", "/fields",
    "/logs/search", "/logs/histogram", "/logs/facet",
    "/logs/row/{row_ref}", "/logs/context", "/logs/aggregate",
    "/metrics/query", "/metrics/query_range",
    "/metrics/labels", "/metrics/label/{name}/values",
    "/metrics/series", "/metrics/metadata",
    "/exports", "/exports/{task_id}", "/queries/{client_query_id}",
]
missing = [e for e in required_endpoints if e not in doc["paths"]]
print(f"missing M2 endpoints: {missing if missing else 'none'}")

sys.exit(1 if (broken or missing) else 0)
