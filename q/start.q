/ Define the table, then restore the saved snapshot when present.
\l q/schema.q
if[`trades in key `:data;trades:get `:data/trades];
