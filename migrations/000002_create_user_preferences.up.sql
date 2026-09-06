-- Per-channel opt-in/out. Absence of a row for (user_id, channel) means
-- "enabled" — preferences only need a row once a user actively flips a
-- channel off, so a brand-new user needs no pre-populated rows at all.
CREATE TABLE user_preferences (
    user_id     TEXT NOT NULL,
    channel     TEXT NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT true,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (user_id, channel)
);

-- Do-not-disturb window: no channel is delivered to while the current time
-- (in `timezone`) falls between start_min and end_min, both stored as
-- minutes-since-midnight (0-1439) rather than a SQL TIME column, so scanning
-- them in Go is just an int — no time-type/driver friction. end_min may be
-- less than start_min to mean a window that wraps past midnight (e.g.
-- 22:00-07:00 is start_min=1320, end_min=420).
CREATE TABLE user_dnd_windows (
    user_id     TEXT PRIMARY KEY,
    start_min   SMALLINT NOT NULL CHECK (start_min BETWEEN 0 AND 1439),
    end_min     SMALLINT NOT NULL CHECK (end_min BETWEEN 0 AND 1439),
    timezone    TEXT NOT NULL DEFAULT 'UTC',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
