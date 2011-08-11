SELECT json('"hello"');
SELECT json($$"hello\u266Bworld"$$);
SELECT json($$"hello\u266bworld"$$);
SELECT json($$"hello♫world"$$);
SELECT json($$      "hello world"    $$);
SELECT json($$    {  "hello" : "world"}    $$);
SELECT json($$    {  "hello" : "world", "bye": 0.0001  }    $$);
SELECT json($$    {  "hello" : "world",
								"bye": 0.0000001
}    $$);
SELECT json($$    {  "hello" : "world"
,
"bye"
: [-0.1234e1, 12345e0]		}    $$);
SELECT json($$"\u007E\u007F\u0080"$$)::text = E'\"\u007E\u007F\u0080\"';
SELECT json($$"\u00FE\u00FF\u0100"$$);
SELECT json($$"\uD835\uDD4E"$$);
SELECT json($$"\uD835\uD835"$$);

-- These should be turned into single-character escapes.
SELECT json($$"\u0022"$$)::text = $$"\""$$;
SELECT json($$"\u005c"$$)::text = $$"\\"$$;
SELECT json($$"\u0008"$$)::text = $$"\b"$$;
SELECT json($$"\u000c"$$)::text = $$"\f"$$;
SELECT json($$"\u000a"$$)::text = $$"\n"$$;
SELECT json($$"\u000d"$$)::text = $$"\r"$$;
SELECT json($$"\u0009"$$)::text = $$"\t"$$;
