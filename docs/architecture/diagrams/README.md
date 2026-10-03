# Architecture diagrams (draw.io)

> **As built at `7d8833d`** (`develop`, 2026-08-16), like the rest of [docs/architecture](../README.md).

These are native draw.io files (uncompressed XML) for the main ASCII figures. Open them in the
draw.io desktop app, at app.diagrams.net, or with the draw.io editor extension in VS Code / JetBrains.
GitHub shows a `.drawio` file as XML rather than as a picture, so the docs link to these files and
keep their ASCII figures inline.

Edges and boxes use one colour per status label: green LIVE, amber dashed IDLE, red dotted with a
cross DEAD-AS-WIRED, grey dotted served-never-dialed and grey STUB. Each diagram carries its own
legend; label definitions are in the [README](../README.md#status-labels).

The ASCII figures stay the reviewed source: they diff cleanly, the linter checks them, and terminal
readers see them. A draw.io diagram follows the ASCII figure it sits next to. When a status
changes, update both in the same change.

| File | Shows | Linked from | Maintained by |
|---|---|---|---|
| [system-context.drawio](system-context.drawio) | The client and its external parties, with each interface's status | [README](../README.md#system-context) | hand, in draw.io |
| [topology-shapes.drawio](topology-shapes.drawio) | Shape A and shape B processes and every internal edge (E1-E8, N1, N2) | [README](../README.md#shape-a-and-shape-b-at-a-glance), [03](../03-internal-contracts.md#3-edge-inventory) | hand, in draw.io |
| [target-after-s2.drawio](target-after-s2.drawio) | The plan's two-process target (`plan/architecture.md` §0); no status | [README](../README.md#planned-changes) | hand, in draw.io |
| [block-lifecycle.drawio](block-lifecycle.drawio) | Hops 1-14 of a block as a swimlane, each with its status | [05](../05-block-lifecycle.md#swimlane-overview) | hand, in draw.io |
| [crate-map.drawio](crate-map.drawio) | All workspace crates coloured by band; transitive reduction of the normal deps | [02](../02-crate-map.md#layers-at-a-glance) | generated |
| [crate-map.edges](crate-map.edges) | The crate map's node and edge list, as text | [02](../02-crate-map.md#layers-at-a-glance) | generated |

## Exporting an image

To get a picture for a slide or a PR, export from the draw.io desktop CLI. `-e` embeds the
diagram so the image stays editable; `--theme light` keeps it readable on dark backgrounds.

```bash
drawio -x -f svg -e -b 10 --theme light -o name.drawio.svg name.drawio
drawio -x -f png -e -b 10 -o name.drawio.png name.drawio
```

## Regenerating the crate map

```bash
bash scripts/gen-crate-map.sh          # rewrite crate-map.drawio and crate-map.edges (needs draw.io desktop)
bash scripts/gen-crate-map.sh --check  # exit 1 if crate-map.edges no longer matches cargo metadata
```

The script reads `cargo metadata` and files each crate under one of the seven bands of
[02](../02-crate-map.md#layers-at-a-glance). It lays out the transitive reduction with ELK and
puts the remaining edges on a hidden layer called "implied edges". The output is deterministic,
so re-running it with no dependency change leaves the files byte-identical. It fails when a
workspace member has no band, so a new crate cannot silently drop off the map; add that crate to
`LAYERS` in the script. `--check` needs neither draw.io nor a display, so it could run in CI, but
it is not yet wired into `make lint` or CI.
