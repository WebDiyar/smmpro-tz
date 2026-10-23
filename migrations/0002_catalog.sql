INSERT INTO models (slug, price) VALUES ('image-fast', 2), ('video-long', 25);

INSERT INTO legs (model_slug, provider, priority, max_ref_images, skip_on_moderation) VALUES
    ('image-fast', 'alpha', 1, 4,  false),
    ('image-fast', 'beta',  2, 10, false),
    ('video-long', 'beta',  1, 2,  false),
    ('video-long', 'alpha', 2, 1,  true);
