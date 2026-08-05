#[cfg(test)]
mod tests {
    use cryptomeria_ingest::bitstamp::types::instrument_to_channel;
    use cryptomeria_ingest::okx::types::{PriceLevel, extract_levels, parse_price_level};
    use proptest::prelude::*;

    // Property: parse_price_level returns Some((price, size)) when level has >=2 valid f64 strings
    proptest! {
        #[test]
        fn prop_parse_price_level_valid_two_elements(
            price in any::<f64>().prop_filter("finite", |x| x.is_finite()),
            size in any::<f64>().prop_filter("finite", |x| x.is_finite()),
        ) {
            let level: PriceLevel = vec![price.to_string(), size.to_string()];
            let result = parse_price_level(&level);
            prop_assert!(result.is_some());
            let (p, s) = result.unwrap();
            prop_assert_eq!(p, price);
            prop_assert_eq!(s, size);
        }
    }

    // Property: parse_price_level returns None for empty or single-element level
    proptest! {
        #[test]
        fn prop_parse_price_level_too_few_elements(
            level in prop::collection::vec(any::<String>(), 0..2)
        ) {
            let result = parse_price_level(&level);
            prop_assert!(result.is_none());
        }
    }

    // Property: parse_price_level returns None if first element is not a valid f64
    proptest! {
        #[test]
        fn prop_parse_price_level_invalid_price(
            invalid_price in any::<String>().prop_filter("not f64", |s| s.parse::<f64>().is_err()),
            size in any::<f64>().prop_filter("finite", |x| x.is_finite()),
        ) {
            let level: PriceLevel = vec![invalid_price, size.to_string()];
            let result = parse_price_level(&level);
            prop_assert!(result.is_none());
        }
    }

    // Property: parse_price_level returns None if second element is not a valid f64
    proptest! {
        #[test]
        fn prop_parse_price_level_invalid_size(
            price in any::<f64>().prop_filter("finite", |x| x.is_finite()),
            invalid_size in any::<String>().prop_filter("not f64", |s| s.parse::<f64>().is_err()),
        ) {
            let level: PriceLevel = vec![price.to_string(), invalid_size];
            let result = parse_price_level(&level);
            prop_assert!(result.is_none());
        }
    }

    // Property: extract_levels returns empty vec for missing key
    proptest! {
        #[test]
        fn prop_extract_levels_missing_key(
            key in prop::sample::select(vec!["price".to_string(), "foo".to_string(), "x".to_string()]),
            num in any::<f64>().prop_filter("finite", |x| x.is_finite()),
        ) {
            let data = serde_json::json!({ "price": num, "foo": num, "x": num });
            let result = extract_levels(&data, &key);
            prop_assert_eq!(result, Vec::<PriceLevel>::new());
        }
    }

    // Property: instrument_to_channel is idempotent
    proptest! {
        #[test]
        fn prop_instrument_to_channel_idempotent(
            s in any::<String>()
        ) {
            let first = instrument_to_channel(&s);
            let second = instrument_to_channel(&first);
            prop_assert_eq!(first, second);
        }
    }

    // Property: instrument_to_channel preserves alphanumerics in order
    proptest! {
        #[test]
        fn prop_instrument_to_channel_preserves_alphanumerics(
            s in prop::collection::vec(any::<char>(), 0..50)
        ) {
            let input: String = s.into_iter().collect();
            let result = instrument_to_channel(&input);
            let expected: String = input
                .chars()
                .filter(|c| c.is_alphanumeric())
                .flat_map(|c| c.to_lowercase())
                .collect();
            prop_assert_eq!(result, expected);
        }
    }
}
