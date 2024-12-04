import streamlit as st

# Set page configuration
st.set_page_config(page_title="DispensaryTrack | Real-Time Inventory SaaS", layout="wide")

# Header Section
st.markdown(
    """
    <style>
    .main-header {
        font-size: 40px;
        font-weight: bold;
        text-align: center;
        color: #2F855A;
    }
    .sub-header {
        text-align: center;
        font-size: 18px;
        color: #4A5568;
        margin-bottom: 30px;
    }
    .cta-button {
        display: flex;
        justify-content: center;
        margin-top: 20px;
    }
    .cta-button button {
        background-color: #48BB78;
        color: white;
        padding: 10px 20px;
        font-size: 16px;
        border: none;
        border-radius: 5px;
        cursor: pointer;
        margin-right: 10px;
    }
    .cta-button button:hover {
        background-color: #38A169;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Hero Section
st.markdown(
    """
    <div class="main-header">Track Your Inventory Across All Dispensaries in Real Time</div>
    <div class="sub-header">Simplify your inventory management with automated updates, comprehensive dashboards, and actionable insights.</div>
    """,
    unsafe_allow_html=True,
)

col1, col2, col3 = st.columns([1, 1, 1])

with col2:
    if st.button("Get Started Now"):
        st.write("Redirect to Sign Up Page!")  # Replace with redirection logic

# Features Section
st.markdown("### Key Features")
features = [
    ("Real-Time Inventory Sync", "Keep all dispensary data up to date with automated syncing."),
    ("Actionable Insights", "Analyze trends and make data-driven decisions to maximize revenue."),
    ("Customizable Dashboards", "Focus on the metrics that matter most to your business."),
    ("Multi-Store Support", "Centralized inventory management for all locations."),
]

cols = st.columns(len(features))
for col, (title, description) in zip(cols, features):
    with col:
        st.subheader(title)
        st.write(description)

# How It Works Section
st.markdown("### How It Works")
st.markdown(
    """
    1. **Connect Your Dispensaries**: Seamlessly integrate using APIs.  
    2. **View Data in Real Time**: Access a centralized dashboard with live insights.  
    3. **Act on Insights**: Restock efficiently and maximize revenue.
    """
)

# Pricing Section
st.markdown("### Pricing")
pricing_tiers = {
    "Basic": "$99/month\n- Single dispensary support.",
    "Pro": "$199/month\n- Multi-location support and dashboards.",
    "Enterprise": "Contact Us\n- Custom integrations and analytics.",
}

cols = st.columns(len(pricing_tiers))
for col, (tier, details) in zip(cols, pricing_tiers.items()):
    with col:
        st.subheader(tier)
        st.write(details)

# Testimonials Section
st.markdown("### What Our Customers Say")
st.write(
    """
    > "DispensaryTrack has revolutionized the way we manage inventory. The real-time updates and insights have saved us hours every week!"  
    > — **Happy Customer**
    """
)

# Footer
st.markdown(
    """
    ---
    <div style="text-align: center; color: #4A5568; font-size: 14px;">
        © 2024 DispensaryTrack. All rights reserved. | <a href="#" style="color: #48BB78;">Privacy Policy</a> | <a href="#" style="color: #48BB78;">Terms of Service</a>
    </div>
    """,
    unsafe_allow_html=True,
)
