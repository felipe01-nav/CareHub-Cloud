#!/usr/bin/env python3
"""CareHub: teach the collaboration library (AppFlowy-Collab, open source) the newer
view types the web app uses (Chart, List, Gallery, Feed, Form, Timeline), and point the
server build at this patched copy in vendor/appflowy-collab.

Run from the repository root after cloning AppFlowy-Collab into vendor/appflowy-collab.
Every replacement must match exactly the expected number of times, otherwise the
build stops (so an upstream change can never be patched silently in the wrong place)."""
import re, sys, os

V = 'vendor/appflowy-collab'
REV = 'e59260e524f33104b0ddcd6bb8f6218cad0f7e18'

def patch(path, old, new, count=1):
    with open(path, encoding='utf-8') as f:
        text = f.read()
    found = text.count(old)
    if found != count:
        sys.exit(f'CareHub patch: expected {count} match(es) in {path}, found {found}:\n{old}')
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text.replace(old, new))
    print(f'patched {path}')

# ---- Folder (sidebar) view types: web numbers Chart 5 ... Timeline 10
fv = f'{V}/collab-folder/src/view.rs'
patch(fv, '  Calendar = 3,\n  Chat = 4,\n}',
      '  Calendar = 3,\n  Chat = 4,\n  Chart = 5,\n  List = 6,\n  Gallery = 7,\n  Feed = 8,\n  Form = 9,\n  Timeline = 10,\n}')
patch(fv, '      ViewLayout::Grid | ViewLayout::Board | ViewLayout::Calendar\n    )',
      '      ViewLayout::Grid\n        | ViewLayout::Board\n        | ViewLayout::Calendar\n        | ViewLayout::Chart\n'
      '        | ViewLayout::List\n        | ViewLayout::Gallery\n        | ViewLayout::Feed\n'
      '        | ViewLayout::Form\n        | ViewLayout::Timeline\n    )')
patch(fv, '      4 => Ok(ViewLayout::Chat),\n',
      '      4 => Ok(ViewLayout::Chat),\n      5 => Ok(ViewLayout::Chart),\n      6 => Ok(ViewLayout::List),\n'
      '      7 => Ok(ViewLayout::Gallery),\n      8 => Ok(ViewLayout::Feed),\n      9 => Ok(ViewLayout::Form),\n'
      '      10 => Ok(ViewLayout::Timeline),\n')

# ---- Database view types: web numbers Chart 3 ... Timeline 8
NEW = [('Chart', 3), ('List', 4), ('Gallery', 5), ('Feed', 6), ('Form', 7), ('Timeline', 8)]
dl = f'{V}/collab-database/src/views/layout.rs'
patch(dl, '  Calendar = 2,\n}', '  Calendar = 2,\n' + ''.join(f'  {n} = {i},\n' for n, i in NEW) + '}')
patch(dl, '      DatabaseLayout::Calendar => "2",\n',
      '      DatabaseLayout::Calendar => "2",\n' + ''.join(f'      DatabaseLayout::{n} => "{i}",\n' for n, i in NEW))
patch(dl, '      "2" => Ok(DatabaseLayout::Calendar),\n',
      '      "2" => Ok(DatabaseLayout::Calendar),\n' + ''.join(f'      "{i}" => Ok(DatabaseLayout::{n}),\n' for n, i in NEW))
patch(dl, '      2 => DatabaseLayout::Calendar,\n',
      '      2 => DatabaseLayout::Calendar,\n' + ''.join(f'      {i} => DatabaseLayout::{n},\n' for n, i in NEW))
fs = f'{V}/collab-database/src/fields/field_settings.rs'
patch(fs, '    DatabaseLayout::Calendar => FieldVisibility::HideWhenEmpty,\n  }',
      '    DatabaseLayout::Calendar => FieldVisibility::HideWhenEmpty,\n'
      '    DatabaseLayout::Timeline => FieldVisibility::HideWhenEmpty,\n'
      '    DatabaseLayout::Chart\n    | DatabaseLayout::List\n    | DatabaseLayout::Gallery\n'
      '    | DatabaseLayout::Feed\n    | DatabaseLayout::Form => FieldVisibility::AlwaysShown,\n  }')

# ---- Server build: use the patched copy instead of downloading the original
with open('Cargo.toml', encoding='utf-8') as f:
    cargo = f.read()
pat = re.compile(r'^(collab(?:-[a-z]+)?) = \{ git = "https://github\.com/AppFlowy-IO/AppFlowy-Collab", rev = "' + REV + r'" \}$', re.M)
crates = pat.findall(cargo)
if len(crates) != 8:
    sys.exit(f'CareHub patch: expected 8 collab crates in Cargo.toml, found {len(crates)}')
for c in crates:
    if not os.path.isdir(f'{V}/{c}'):
        sys.exit(f'CareHub patch: missing {V}/{c}')
cargo = pat.sub(lambda m: f'{m.group(1)} = {{ path = "{V}/{m.group(1)}" }}', cargo)
if cargo.count('[workspace]\nmembers = [') != 1:
    sys.exit('CareHub patch: [workspace] block not found in Cargo.toml')
cargo = cargo.replace('[workspace]\nmembers = [', '[workspace]\nexclude = ["vendor"]\nmembers = [')
with open('Cargo.toml', 'w', encoding='utf-8') as f:
    f.write(cargo)
print('Cargo.toml now uses', ', '.join(crates), 'from', V)
