# Player discovery

The graph explorer searches the pinned **2025–26 Aston Villa and Liverpool roster**
(57 players). Full-league fixtures do not imply full-league player coverage.

Player lookup normalizes case, diacritics, punctuation and spacing. Display names,
full names and full-name tokens are accepted: `ekitike` resolves Hugo Ekitiké,
`Alisson` resolves Alisson Becker (displayed as A.Becker), and `Salah` resolves
Mohamed Salah. Unique partial names of at least three characters also resolve.
The native input suggestions expose stored full names.

If an input is misspelled, the API returns up to five closest stored names, which
the user can select. Suggestions use a deterministic SequenceMatcher score of at
least 0.74 against normalized aliases; single-word queries also compare name
tokens. Multiple exact or partial matches require selection. Fuzzy matches never
silently select a player. Clicking a suggestion sends its canonical player ID.

Examples:

| Input | Behavior |
| --- | --- |
| `ekitike` | Opens Hugo Ekitiké despite the missing accent. |
| `allison` | Suggests Alisson Becker; select the suggestion to open his graph. |
| `Luis Diaz` | No stored match in this roster snapshot. |

The `/api/graph/player/{name}` route returns 200 with the existing graph shape
when resolved, 409 with `detail.suggestions` when selection is required, and 404
when no supported match is found. Database/service failures remain 503 and are
shown as temporary failures, separately from missing coverage. Player projections
include the public `full_name` field; internal provenance fields remain excluded.

Inputs are bounded to 120 characters for matching; short fragments do not receive
fuzzy suggestions. Matching reads only current-season, MVP-managed player records
and does not expand or modify the source roster. A close name is not evidence of
player identity, and absent players are not substituted with unrelated records.
