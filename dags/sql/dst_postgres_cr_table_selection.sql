CREATE TABLE IF NOT EXISTS selection_dst (
    pk              SERIAL PRIMARY KEY,
    id              integer,
    name            text,
    start_date      date,
    end_date        date,
    url             varchar(254),
    address         varchar(254),
    enum_procedure_type varchar(254)
);

CREATE TABLE IF NOT EXISTS selection_dst_lots (
    selection_pk INTEGER REFERENCES selection_dst (pk),
    name    varchar(254),
    amount  NUMERIC(12, 2)
);