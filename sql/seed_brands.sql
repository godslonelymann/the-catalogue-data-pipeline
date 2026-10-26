-- The 10 tracked brands. Run as postgres after catalog_schema.sql.
-- Safe to run more than once.

INSERT INTO catalog.brands (id, name, base_url) VALUES
    (1,  'Fugazee',          'https://www.fugazee.com'),
    (2,  'Genrage',          'https://genrage.com'),
    (3,  'Natty Garb',       'https://www.nattygarb.com'),
    (4,  'Sullitt',          'https://sullitt.com'),
    (5,  'Warping Theories', 'https://warpingtheories.com'),
    (6,  'Arkar',            'https://arkar.in'),
    (7,  'NoughtOne',        'https://www.noughtone.in'),
    (8,  'Farak',            'https://www.farak.co'),
    (9,  'Offscript',        'https://offscript.co.in'),
    (10, 'Urban Monkey',     'https://www.urbanmonkey.com')
ON CONFLICT (id) DO NOTHING;

-- Keep the SERIAL counter after the explicit ids.
SELECT setval('catalog.brands_id_seq', (SELECT max(id) FROM catalog.brands));
