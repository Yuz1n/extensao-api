CREATE TABLE IF NOT EXISTS streamer (
    id SERIAL PRIMARY KEY,
    "user" VARCHAR(255) NOT NULL,
    link VARCHAR(255),
    id_streamer VARCHAR(255) NOT NULL UNIQUE
);

-- Exemplo: inserir um streamer
-- INSERT INTO streamer ("user", link, id_streamer) VALUES ('NomeDoStreamer', 'RgjGZ6J', 'username_kick');
