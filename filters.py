def update_filter_options_dutchie(df):
    filter_options = {
        "brands": ["ALL"],
        "dispensaries": ["ALL"],
        "categories": ["ALL"],
    }

    if 'brand' in df.columns:
        filter_options["brands"] += df['brand'].unique().tolist()

    if 'dispensary_name' in df.columns:
        filter_options["dispensaries"] += df['dispensary_name'].unique().tolist()

    if 'category' in df.columns:
        filter_options["categories"] += df['category'].unique().tolist()

    return filter_options


def update_filter_options_typesense(df):
    filter_options = {
        "brands": ["ALL"],
        "locations": ["ALL"],
        "categories": ["ALL"],
    }

    if 'Brand' in df.columns:
        filter_options["brands"] += df['Brand'].unique().tolist()

    if 'Location' in df.columns:
        filter_options["locations"] += df['Location'].unique().tolist()

    if 'Category' in df.columns:
        filter_options["categories"] += df['Category'].unique().tolist()

    return filter_options


def update_filter_options_curaleaf(df):
    filter_options = {
        "dispensaries": ["ALL"],
        "categories": ["ALL"],
    }

    if 'dispensary_name' in df.columns:
        filter_options["dispensaries"] += df['dispensary_name'].unique().tolist()

    if 'type' in df.columns:
        filter_options["categories"] += df['type'].unique().tolist()

    return filter_options
