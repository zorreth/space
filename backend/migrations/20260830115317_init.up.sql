CREATE TABLE users (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE chunks (
    chunk_x INT NOT NULL,
    chunk_y INT NOT NULL,
    colors_data BYTEA NOT NULL,
    author_ids_data BYTEA NOT NULL,
    timestamps_data BYTEA NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (chunk_x, chunk_y)
);
