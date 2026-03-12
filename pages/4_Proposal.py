import streamlit as st
import os

st.set_page_config(page_title="Proposal", layout="wide")

st.title("📄 Project Proposal")

# Get path to the README.md in the root directory
current_dir = os.path.dirname(os.path.abspath(__file__))
readme_path = os.path.join(os.path.dirname(current_dir), "README.md")

if os.path.exists(readme_path):
    with open(readme_path, "r", encoding="utf-8") as f:
        readme_content = f.read()
    st.markdown(readme_content)
else:
    st.error("README.md not found.")
