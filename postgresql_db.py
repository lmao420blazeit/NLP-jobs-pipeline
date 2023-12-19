import psycopg

# Connect to an existing database
with psycopg.connect("dbname=test user=postgres", password="4202") as conn:

    # Open a cursor to perform database operations
    with conn.cursor() as cur:

        # Execute a command: this creates a new table
        cur.execute("""
            CREATE TABLE test (
                id serial PRIMARY KEY,
                slug text,
                company_name text,
                title text,
                description text,
                is_remote text,
                url text, 
                tags text array,
                job_types text array, 
                location text,
                tokens text array)
            """)

        # Pass data to fill a query placeholders and let Psycopg perform
        # the correct conversion (no SQL injections!)
        cur.execute(
            "INSERT INTO test (slug, company_name, title, description, is_remote, tags) VALUES (%s, %s)",
            ("kaufmannischer-mitarbeiter-hofgeismar-154258", "Nexus Personalmanagement GmbH", "Kaufmännische:r Mitarbeiter (m/w/w)", 
             "Als inhabergefhrtes Unternehmen verstehen wir uns als DeinPartner, wenn es darum geht, neue berufliche Herausforderungenund Chancen zu finden.", "false",
             "false url", [], [], "a", []))

        # Query the database and obtain data as Python objects.
        cur.execute("SELECT * FROM test")
        cur.fetchone()
        # will return (1, 100, "abc'def")

        # You can use `cur.fetchmany()`, `cur.fetchall()` to return a list
        # of several records, or even iterate on the cursor
        for record in cur:
            print(record)

        # Make the changes to the database persistent
        conn.commit()