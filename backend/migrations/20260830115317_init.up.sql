CREATE TABLE users (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE chunks (
    chunk_x BIGINT NOT NULL,
    chunk_y BIGINT NOT NULL,
    colors_data BYTEA NOT NULL,
    author_ids_data BYTEA NOT NULL,
    timestamps_data BYTEA NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (chunk_x, chunk_y)
);
