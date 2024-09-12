def update_filter_options_dutchie(df):
    return {
        "brands": ["ALL"] + df['brand'].unique().tolist(),
        "dispensaries": ["ALL"] + df['dispensary_name'].unique().tolist(),
        "categories": ["ALL"] + df['category'].unique().tolist(),
    }

def update_filter_options_typesense(df):
    return {
        "brands": ["ALL"] + df['Brand'].unique().tolist(),
        "locations": ["ALL"] + df['Location'].unique().tolist(),
        "categories": ["ALL"] + df['Category'].unique().tolist(),
    }
