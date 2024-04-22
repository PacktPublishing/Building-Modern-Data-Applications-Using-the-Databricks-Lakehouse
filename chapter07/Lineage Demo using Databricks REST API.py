# Databricks notebook source
# MAGIC %md
# MAGIC ## Generate Data
# MAGIC To illustrate retrieving lineage from the Unity Catalog using the Databricks REST API, we'll create a very basic parent and child tables to work with.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, let's create a new Catalog
# MAGIC CREATE CATALOG IF NOT EXISTS <REPLACE_ME>;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Next, well create a schema to hold the tables
# MAGIC CREATE SCHEMA IF NOT EXISTS <REPLACE_ME>.lineage_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a parent table from where our dataset will originate from
# MAGIC CREATE TABLE IF NOT EXISTS <REPLACE_ME>.lineage_demo.youtube_channels
# MAGIC (channel_id INTEGER, num_subscribers INTEGER, language STRING, category STRING, country_of_origin STRING);
# MAGIC
# MAGIC -- Let's populate the newly created table with a few rows of data
# MAGIC INSERT INTO <REPLACE_ME>.lineage_demo.youtube_channels
# MAGIC VALUES (10054, 249000000, "Hindi", "Music", "India"),
# MAGIC        (200054, 184000000, "English", "Entertainment", "United States of America"),
# MAGIC        (429384, 165000000, "English", "Education", "United States of America"),
# MAGIC        (237100012, 163000000, "Hindi", "Entertainment", "India"),
# MAGIC        (207775, 114000000, "English", "Entertainment", "Ukraine");
# MAGIC
# MAGIC -- Ok, let's take a quick look at how the parent dataset looks!
# MAGIC SELECT *
# MAGIC   FROM <REPLACE_ME>.lineage_demo.youtube_channels

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from <REPLACE_ME>.opendatasets.most_subscribed_youtube_channels;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Next, let's create a lookup table that will contain more information
# MAGIC -- on the channel like the channel name and artist information
# MAGIC CREATE TABLE IF NOT EXISTS <REPLACE_ME>.lineage_demo.youtube_channel_artists
# MAGIC (artist_id INTEGER, artist_name STRING, youtube_channel_id INTEGER, youtube_channel_name STRING);
# MAGIC
# MAGIC -- Let's add a few rows of data
# MAGIC INSERT INTO <REPLACE_ME>.lineage_demo.youtube_channel_artists
# MAGIC VALUES (10045, "Bhushan Kumar", 10054, "T-Series"),
# MAGIC        (10046, "Jimmy Donaldson", 200054, "MrBeast"),
# MAGIC        (10047, "Jay Jeon", 429384, "Cocomelon"),
# MAGIC        (10048, "Sony Entertainment Television India", 237100012, "Sony Entertainment Television India"),
# MAGIC        (10049, "Diana Kidisyuk", 207775, "Kids Diana Show");

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Finally, let's join together these 2 datasets
# MAGIC DROP TABLE IF EXISTS <REPLACE_ME>.lineage_demo.combined_table;
# MAGIC CREATE TABLE <REPLACE_ME>.lineage_demo.combined_table
# MAGIC SELECT c.youtube_channel_name AS channel_name,
# MAGIC        c.artist_name,
# MAGIC        CONCAT('A YouTube channel by ', c.artist_name, ' dedicated to videos about ', p.category) AS description,
# MAGIC        p.num_subscribers,
# MAGIC        p.language,
# MAGIC        p.category,
# MAGIC        p.country_of_origin
# MAGIC   FROM <REPLACE_ME>.lineage_demo.youtube_channels p
# MAGIC   JOIN <REPLACE_ME>.lineage_demo.youtube_channel_artists c
# MAGIC     ON p.channel_id = c.youtube_channel_id;
# MAGIC
# MAGIC -- Let's take a final look at our downstream table
# MAGIC SELECT *
# MAGIC   FROM <REPLACE_ME>.lineage_demo.combined_table;
