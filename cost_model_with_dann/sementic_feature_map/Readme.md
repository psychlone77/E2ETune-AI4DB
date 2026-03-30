/*
1. Important Notes

"sum" → add values
"max" → take max
"ratio" → first / second
"direct_reuse" → reuse same value but different semantic meaning
"none" → missing in that DB
*/

2. Best Practice (What You Should Do)

For every semantic feature, define:

(value, mask)

Where:

mask = 1 → reliable
mask = 0 → not present
mask ∈ (0,1) → approximate (optional advanced)

