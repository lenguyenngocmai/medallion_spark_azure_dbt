# Structure model project

1. **Staging Model:**
    - The staging model is used to prepare the atomic building blocks of your data. Its goal is to clean and prepare individual source-conformed concepts for downstream usage.
    - The staging model should have a 1-1 relationship to the source table.
    - Use only base transformation such as column renaming, type casting, and categorization (case when). Avoid joins and aggregations in the staging layer.
    - Use a naming convention for your staging models. For example: `stg__[source]__[entity]s.sql`.
    - Be materialized as a view.
2. **Intermediate Model:**
    - The intermediate model is used to create business-conformed models to break up complexity from the mart models.
    - Split your models up into subdirectories not by their source system, but by their area of business concern.
    - Use a naming convention for your intermediate models. For example: `int_[entity]s_[verb]s.sql`, where the best guiding principle is to think about verbs (e.g. pivoted, aggregated_to_user, joined).
3. **Mart Model:**
    - The mart model is used to create denormalized, aggregated, and business-conformed datasets for consumption.
    - Use a naming convention for your mart models. For example: `dim_[entity]s.sql` for dimensions and `fact_[entity]s.sql` for fact tables.
4. **Incremental Model:**
    - Use incremental models to speed up the build process and reduce rebuild times. In DBT, incremental models are used to update the data warehouse incrementally instead of rebuilding it entirely with each run.

By following these best practices, you can create a well-organized, efficient, and maintainable data modeling layer in DBT.

To dive deeper into this topic, I recommend checking out the [modeling best practices](https://docs.getdbt.com/docs/guides/best-practices#best-practices-in-dbt-projects) in the DBT documentation. They provide more details and examples on how to structure your data modeling layer.

There is also a [great article](https://discourse.getdbt.com/t/how-we-structure-our-dbt-projects/355) on the DBT Discourse that provides a detailed explanation of how to structure your DBT projects.

Finally, it's important to note that every organization's data warehouse is unique, so you should tailor your data modeling approach to fit your specific needs. However, following these best practices can help you create a solid foundation to build upon.

Let me know if you have any further questions or if there's anything else I can help you with!
