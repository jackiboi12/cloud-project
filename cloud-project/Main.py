import ydl_utils
import pandas as pd
import numpy
import streamlit as st
from upload import upload_csv

if "initial_submit" not in st.session_state:
    st.session_state.initial_submit = False

def initial_submit():
    st.session_state.initial_submit = True

st.title("YouTube Comment Analytics")

@st.cache_data(show_spinner = False)
def extract_entries_for_url(channel_url):
    list_dict = []
    with st.status("Downloading data..."):
        st.write("Starting Scrape")
        entries = ydl_utils.ydl_get_entries(channel_url)
        st.write("Scrape Done")
    if "_type" in entries[0]:
        if entries[0]["_type"] == "playlist":
            entries = entries[0]["entries"]
    for entry in entries:
        if entry:
            best_format = entry["formats"][-2].get("format", "")
            filesize = entry["formats"][-2].get("filesize", "")
            list_dict.append(
                {
                    "author" : entry.get("uploader", ""),
                    "channel_url" : entry.get("uploader_url", ""),
                    "title": entry.get("title", ""),
                    "webpage_url" : entry.get("webpage_url", ""),
                    "view_count" : entry.get("view_count", ""),
                    "like_count" : entry.get("like_count", ""),
                    "duration" : entry.get("duration", ""),
                    "upload_date" : entry.get("upload_date", ""),
                    "tags" : entry.get("tags", ""),
                    "categories" : entry.get("categories", ""),
                    "description" : entry.get("description", ""),
                    "thumbnail" : entry.get("thumbnail", ""),
                    "best_format" : best_format,
                    "filesize_bytes" : filesize,
                }
            )
    return list_dict

with st.form("initial-submit"):
    url = st.text_input(label = "Please input a valid YouTube Channel URL", value = "")
    submit = st.form_submit_button(label = "Analyse", on_click = initial_submit)

if st.session_state.initial_submit:
    df = pd.DataFrame(extract_entries_for_url(url))
    st.header(df["author"][0])
    convert = {}
    for i in range(2, 6):
        convert[i ** 2] = i
    choice = 4
    if len(df) > 4:
        for i in convert.keys():
            if len(df) > i:
                choice = i
            else:
                break
    k = 0
    num_img = convert[choice]
    for i in range(num_img):
        cols = st.columns(num_img)
        for x in cols:
            with x:
                st.image(df["thumbnail"][k], use_column_width = True)
                k += 1
    st.dataframe(df)
    st.subheader("Download and upload to S3")
    csv_name = st.text_input("Enter CSV File Name:")
    datasource_name = st.text_input("Enter Datasource Name:")

    if  datasource_name  and csv_name:
        upload_csv(df, datasource_name, csv=csv_name + ".csv")
