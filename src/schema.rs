table! {
    please_ids (id) {
        id -> Integer,
        creation -> Timestamptz,
        expiry -> Timestamptz,
        title -> Text,
        refresh_count -> Integer,
    }
}
