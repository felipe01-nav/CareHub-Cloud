#!/usr/bin/env python3
"""CareHub audit of the server: list HTTP routes and branding/social text."""
import os, re, sys
src, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
routes = []
res_re = re.compile(r'web::(scope|resource)\(\s*"([^"]*)"')
meth_re = re.compile(r'web::(get|post|put|patch|delete)\(\)')
for root, dirs, fs in os.walk(os.path.join(src, 'src')):
    for fn in fs:
        if not fn.endswith('.rs'):
            continue
        p = os.path.join(root, fn)
        text = open(p, encoding='utf-8', errors='replace').read()
        scopes = [m.group(2) for m in res_re.finditer(text) if m.group(1) == 'scope']
        prefix = scopes[0] if scopes else ''
        for m in res_re.finditer(text):
            if m.group(1) != 'resource':
                continue
            window = text[m.end():m.end() + 600]
            nxt = res_re.search(window)
            if nxt:
                window = window[:nxt.start()]
            methods = sorted(set(x.group(1).upper() for x in meth_re.finditer(window)))
            path = m.group(2)
            full = path if path.startswith('/api') else prefix + path
            routes.append(f"{full}\t{','.join(methods)}\t{os.path.relpath(p, src)}")
with open(os.path.join(out, 'server-routes.txt'), 'w') as f:
    f.write('\n'.join(sorted(set(routes))) + '\n')
# scopes per file for checking prefixes
with open(os.path.join(out, 'server-scopes.txt'), 'w') as f:
    for root, dirs, fs in os.walk(os.path.join(src, 'src')):
        for fn in fs:
            if fn.endswith('.rs'):
                p = os.path.join(root, fn)
                sc = [m.group(2) for m in res_re.finditer(open(p, encoding='utf-8', errors='replace').read()) if m.group(1) == 'scope']
                if sc:
                    f.write(f"{os.path.relpath(p, src)}\t{' | '.join(sc)}\n")
# branding / social text in templates and libs (non-code-identifier matches)
pat = re.compile(r"twitter|discord|reddit|youtube|linkedin|follow us|appflowy\.(io|com)|@appflowy|AppFlowy", re.I)
hits = []
for base in ('libs/workspace-template', 'src', 'services', 'libs'):
    bp = os.path.join(src, base)
    if not os.path.isdir(bp):
        continue
    for root, dirs, fs in os.walk(bp):
        dirs[:] = [d for d in dirs if d not in ('target', 'tests', '.sqlx')]
        for fn in fs:
            if not fn.endswith(('.rs', '.json', '.md', '.html', '.txt', '.toml')):
                continue
            p = os.path.join(root, fn)
            try:
                for i, line in enumerate(open(p, encoding='utf-8', errors='replace')):
                    s = line.strip()
                    if s.startswith(('use ', '//', 'mod ', 'pub mod', '#[')):
                        continue
                    if re.search(r'"[^"]*(' + pat.pattern + r')[^"]*"', s, re.I):
                        hits.append(f"{os.path.relpath(p, src)}:{i+1}\t{s[:200]}")
            except Exception:
                pass
with open(os.path.join(out, 'server-brand.txt'), 'w') as f:
    f.write('\n'.join(sorted(set(hits))) + '\n')
# list template asset files
with open(os.path.join(out, 'server-template-files.txt'), 'w') as f:
    tp = os.path.join(src, 'libs/workspace-template')
    for root, dirs, fs in os.walk(tp):
        for fn in fs:
            f.write(os.path.relpath(os.path.join(root, fn), src) + '\n')
print(f"routes={len(routes)} brand={len(hits)}")
