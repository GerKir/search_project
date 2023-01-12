create table info
(
    id     serial primary key,
    topic  varchar(255),
    "text" text,
    "tag"  varchar(15)
);

