from flask import Flask, flash, redirect, render_template, request, url_for, jsonify
import psycopg2
from psycopg2 import errors, IntegrityError
from db_config import DB_CONFIG
from datetime import date
import os
from data_ingestion import load_production_data_if_needed
from analytics import (
    init_analytics_views, get_market_overview, get_host_performance,
    get_neighbourhood_analytics, get_price_trends, get_top_listings,
    refresh_analytics_views
)
# Import recommendation module (modular - can be easily removed)
from recommendations import recommendation_engine


app = Flask(__name__)
# Secret key for session/flash support (set this to a secure random value)
app.secret_key = os.urandom(24)

# Initialize database on startup (only once)
def initialize_app():
    """Initialize database schema and data on first run"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check if database is already initialized by checking for Listing table
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'listing'
            );
        """)
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            print("Database not initialized. Initializing schema...")
            cur.close()
            conn.close()
            
            # Initialize schema
            init_db()
            
            # Load production data
            print("Loading production data...")
            load_production_data_if_needed()
            
            # Initialize analytics views
            print("Initializing analytics views...")
            init_analytics_views()
            print("Database initialization complete!")
        else:
            print("Database already initialized.")
            cur.close()
            conn.close()
    except Exception as e:
        print(f"Error during initialization: {e}")
        print("You may need to initialize the database manually.")

# Run initialization when app starts (before first request)
with app.app_context():
    initialize_app()

def get_db_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

# Initializes the database schema from data.sql.
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    with open('data.sql', 'r') as f:
        ddl_script = f.read()
    
    # Execute the entire script at once to handle dollar-quoted strings properly
    cur.execute(ddl_script)
    conn.commit()
    cur.close()
    conn.close()


# Route for homepage.
@app.route('/')
def home():
    # Fetch top 3 listings per neighbourhood by avg rating DESC, price ASC
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
    SELECT
    l.listing_id,
    l.name,
    l.price,
    n.name AS neighbourhood,
    l.minimum_nights AS min_nights,
    COALESCE(AVG(r.rating), 0) AS avg_rating
    FROM Listing l
    JOIN Neighbourhood n ON n.listing_id = l.listing_id
    LEFT JOIN Review r ON r.listing_id = l.listing_id
    GROUP BY l.listing_id, l.name, l.price, n.name
    ORDER BY avg_rating DESC, l.price ASC
    LIMIT 3;
    """)
    rows = cur.fetchall()
    top_listings = [
        {
            "listing_id":    r[0],
            "name":          r[1],
            "price":         float(r[2]),
            "neighbourhood": r[3],
            "min_nights":    r[4],
            "avg_rating":    float(r[5]),
        }
        for r in rows
    ]
    cur.close()
    conn.close()
    
    return render_template("home.html", top_listings=top_listings)

# Route for standalone analytics dashboard
@app.route('/analytics')
def analytics_dashboard():
    try:
        market_overview = get_market_overview()
        host_performance = get_host_performance(limit=15)
        neighbourhood_analytics = get_neighbourhood_analytics(limit=20)
        price_trends = get_price_trends()
        top_performing_listings = get_top_listings(limit=15)
        
        analytics_data = {
            'market_overview': market_overview,
            'host_performance': host_performance,
            'neighbourhood_analytics': neighbourhood_analytics,
            'price_trends': price_trends,
            'top_performing_listings': top_performing_listings
        }
    except Exception as e:
        print(f"Error fetching analytics data: {e}")
        analytics_data = {}
    
    return render_template("analytics.html", analytics=analytics_data)

# API route for analytics data (for interactive charts)
@app.route('/api/analytics')
def api_analytics():
    try:
        market_overview = get_market_overview()
        host_performance = get_host_performance(limit=50)
        neighbourhood_analytics = get_neighbourhood_analytics(limit=50)
        price_trends = get_price_trends()
        top_performing_listings = get_top_listings(limit=50)
        
        return jsonify({
            'market_overview': market_overview,
            'host_performance': host_performance,
            'neighbourhood_analytics': neighbourhood_analytics,
            'price_trends': price_trends,
            'top_performing_listings': top_performing_listings
        })
    except Exception as e:
        print(f"Error fetching analytics data: {e}")
        return jsonify({'error': 'Failed to fetch analytics data'}), 500

# API route for host performance data
@app.route('/api/analytics/host-performance')
def api_host_performance():
    try:
        host_performance = get_host_performance(limit=50)
        return jsonify(host_performance)
    except Exception as e:
        print(f"Error fetching host performance data: {e}")
        return jsonify({'error': 'Failed to fetch host performance data'}), 500

# API route for price trends data
@app.route('/api/analytics/price-trends')
def api_price_trends():
    try:
        price_trends = get_price_trends()
        return jsonify(price_trends)
    except Exception as e:
        print(f"Error fetching price trends data: {e}")
        return jsonify({'error': 'Failed to fetch price trends data'}), 500

# API route for neighbourhood analytics data
@app.route('/api/analytics/neighbourhood')
def api_neighbourhood_analytics():
    try:
        neighbourhood_analytics = get_neighbourhood_analytics(limit=50)
        return jsonify(neighbourhood_analytics)
    except Exception as e:
        print(f"Error fetching neighbourhood analytics data: {e}")
        return jsonify({'error': 'Failed to fetch neighbourhood analytics data'}), 500

# Route to manually refresh analytics views
@app.route('/refresh-analytics')
def refresh_analytics():
    try:
        success = refresh_analytics_views()
        if success:
            flash('Analytics data refreshed successfully!', 'success')
        else:
            flash('Failed to refresh analytics data.', 'error')
    except Exception as e:
        flash(f'Error refreshing analytics: {str(e)}', 'error')
    
    return redirect(url_for('home'))

# Route to add sample data to the listings database.
@app.route('/add-sample')
def add_sample():
    conn = get_db_connection()
    cur = conn.cursor()
    message = "Sample data inserted successfully."

    try:
        with open('sample.sql', 'r') as f:
            sql_script = f.read()

        for stmt in sql_script.split(';'):
            stmt = stmt.strip()
            if not stmt:
                continue
            cur.execute(stmt + ';')

        conn.commit()

    except IntegrityError as e:
        conn.rollback()
        # If it's a duplicate‐key error, let the user know the data was already added
        if isinstance(e, errors.UniqueViolation):
            message = "Sample data has already been added."
        else:
            message = f"Error inserting sample data: {e.pgerror}"
            
    finally:
        cur.close()
        conn.close()

    return render_template('add_sample.html', message=message)

# Route for viewing the listings with search, sort, and filter functionality.
@app.route('/view-listings')
def view_listings():
    conn = get_db_connection()
    cur = conn.cursor()

    # 1. Read all params
    search = request.args.get('search', type=str)
    neighbourhood = request.args.get('neighbourhood', type=str)
    room_type     = request.args.get('room_type', type=str)
    price_min     = request.args.get('price_min', type=float)
    price_max     = request.args.get('price_max', type=float)
    min_nights    = request.args.get('min_nights', type=int)
    sort_by       = request.args.get('sort_by', type=str)
    sort_order    = request.args.get('sort_order', type=str)
    user_lat      = request.args.get('lat', type=float);
    user_lng      = request.args.get('lng', type=float);
    radius_km     = request.args.get('radius_km', type=float);
    preset_place  = request.args.get('preset_place',  type=str)
    page          = request.args.get('page', type=int) or 1
    per_page      = 20

    # 2. Build base query (join on Neighbourhood.listing_id)
    base_query = """
    SELECT
      l.listing_id,
      l.name,
      l.price,
      l.room_type,
      l.minimum_nights,
      n.name             AS neighbourhood,
      AVG(r.rating)      AS avg_rating,
      COUNT(r.review_id) AS review_count,
      ST_Y(l.geopoint::geometry) AS lat,
      ST_X(l.geopoint::geometry) AS lng
    FROM Listing l
    JOIN Neighbourhood n
      ON n.listing_id = l.listing_id
    LEFT JOIN Review r
      ON r.listing_id = l.listing_id
    """

    group_by = """
      GROUP BY
        l.listing_id, l.name, l.price, l.room_type, n.name
    """

    where_clauses = []
    params = []

    # 3a. Search listing name and neighbourhood based on pattern entered by user.
    if search:
        wildcard = f"%{search}%"
        where_clauses.append("(l.name ILIKE %s OR n.name ILIKE %s)")
        params.extend([wildcard, wildcard])

    # 3b. Apply filters (if provided)
    if neighbourhood:
        where_clauses.append("n.name = %s")
        params.append(neighbourhood)
    if room_type:
        where_clauses.append("l.room_type = %s")
        params.append(room_type)
    if price_min is not None:
        where_clauses.append("l.price >= %s")
        params.append(price_min)
    if price_max is not None:
        where_clauses.append("l.price <= %s")
        params.append(price_max)
    if min_nights is not None:
        where_clauses.append("l.minimum_nights >= %s")
        params.append(min_nights)
    
    # 3c. Geospatial filter
    if user_lat is not None and user_lng is not None and radius_km is not None:
        radius_m = radius_km * 1000
        where_clauses.append("""
          ST_DWithin(
            l.geopoint,
            ST_SetSRID(
              ST_MakePoint(%s, %s), 4326
            )::geography,
            %s
          )
        """)
        params.extend([user_lng, user_lat, radius_m])

    # 4. Stitching the query together.
    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)
        base_query += where_sql
    base_query += group_by

    if sort_by == 'price':
        direction = 'ASC' if sort_order == 'asc' else 'DESC'
        base_query += f"\n  ORDER BY l.price {direction}"
    elif sort_by == 'name':
        direction = 'ASC' if sort_order == 'asc' else 'DESC'
        base_query += f"\n  ORDER BY l.name {direction}"
    else:
       # default ordering
        base_query += """
        ORDER BY avg_rating DESC NULLS LAST,
        l.price ASC
        """
    base_query += f"\nLIMIT %s OFFSET %s;" # End of query
    params_with_pagination = params + [per_page, (page-1)*per_page]

    # 5. Execute and fetch
    cur.execute(base_query, params_with_pagination)
    rows = cur.fetchall()
    listings = [
        {
          'listing_id':   r[0],
          'name':         r[1],
          'price':        float(r[2]),
          'room_type':    r[3],
          'min_nights':   r[4],
          'neighbourhood':r[5],
          'avg_rating':   round(float(r[6]), 2) if r[6] is not None else None,
          'review_count': int(r[7]),
          'lat':          float(r[8]),
          'lng':          float(r[9])
        }
        for r in rows
    ]

    # 6. Fetch total count for pagination
    count_query = f"SELECT COUNT(*) FROM (SELECT l.listing_id FROM Listing l JOIN Neighbourhood n ON n.listing_id = l.listing_id LEFT JOIN Review r ON r.listing_id = l.listing_id {where_sql} {group_by}) AS sub;"
    cur.execute(count_query, params)
    total_count = cur.fetchone()[0]
    total_pages = (total_count + per_page - 1) // per_page
    total_pages = max(1, total_pages) # Prevents weird numbering when no entries are found.

    # 7. Fetch distinct options for your filter dropdowns
    cur.execute("SELECT DISTINCT name FROM Neighbourhood ORDER BY name;")
    neighbourhoods = [n[0] for n in cur.fetchall()]

    cur.execute("SELECT DISTINCT room_type FROM Listing WHERE room_type IS NOT NULL ORDER BY room_type;")
    room_types = [rt[0] for rt in cur.fetchall()]

    cur.close()
    conn.close()

    # 8. Render with everything the template needs
    return render_template(
      'view_listings.html',
      listings=listings,
      neighbourhoods=neighbourhoods,
      room_types=room_types,
      current_filters={
        'search':        search        or '',
        'neighbourhood': neighbourhood or '',
        'room_type':     room_type     or '',
        'price_min':     price_min     or '',
        'price_max':     price_max     or '',
        'min_nights':    min_nights    or '',
        'sort_by':       sort_by       or '',
        'sort_order':    sort_order    or '',
        'lat':           user_lat      or '',
        'lng':           user_lng      or '',
        'radius_km':     radius_km     or '',
        'preset_place':  preset_place  or ''
      },
      page=page,
      total_pages=total_pages
    )

# Route to add listings from users (currently an admin user)
@app.route('/add-listing', methods=['GET', 'POST'])
def add_listing():
    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == 'POST':
        # 1. read form data
        listing_id        = request.form.get('listing_id', type=int)
        host_id           = request.form.get('host_id', type=int)
        name              = request.form.get('name')
        description       = request.form.get('description')
        neighbourhood_overview = request.form.get('neighbourhood_overview')
        room_type         = request.form.get('room_type')
        accommodates      = request.form.get('accommodates', type=int)
        bathrooms         = request.form.get('bathrooms', type=float)
        bathrooms_text    = request.form.get('bathrooms_text')
        bedrooms          = request.form.get('bedrooms', type=int)
        beds              = request.form.get('beds', type=int)
        price             = request.form.get('price', type=float)
        minimum_nights    = request.form.get('minimum_nights', type=int)
        maximum_nights    = request.form.get('maximum_nights', type=int)
        instant_bookable  = bool(request.form.get('instant_bookable'))
        created_date      = date.today()
        last_scraped      = date.today()
        neighbourhood_id  = 0
        neighbourhood_name = request.form.get('neighbourhood_name')
        neighbourhood_group = request.form.get('neighbourhood_group')
        latitude = request.form.get('latitude', type=float)
        longitude = request.form.get('longitude', type=float)

        # 2. Check for duplicate listing_id
        cur.execute('SELECT 1 FROM Listing WHERE listing_id = %s;', (listing_id,))
        if cur.fetchone():
            flash(f'Error: Listing ID {listing_id} already exists.', 'error')
        else:
            # 3. insert into Listing
            try:
                cur.execute(
                    '''INSERT INTO Listing (
                         listing_id, host_id, name, description,
                         neighbourhood_overview, room_type, accommodates,
                         bathrooms, bathrooms_text, bedrooms, beds,
                         price, minimum_nights, maximum_nights,
                         instant_bookable, created_date, last_scraped, geopoint)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                               %s, %s, %s, %s, %s, %s, %s,
                               ST_SetSRID(
                               ST_MakePoint(%s, %s), 4326
                               )::geography
                              )''',
                    (listing_id, host_id, name, description,
                     neighbourhood_overview, room_type, accommodates,
                     bathrooms, bathrooms_text, bedrooms, beds,
                     price, minimum_nights, maximum_nights,
                     instant_bookable, created_date, last_scraped, longitude, latitude)
                )
                cur.execute(
                    'SELECT COALESCE(MAX(neighbourhood_id), 0) + 1 FROM Neighbourhood;'
                )
                neighbourhood_id = cur.fetchone()[0]
                cur.execute(
                    '''INSERT INTO Neighbourhood (
                        neighbourhood_id, listing_id, name, neighbourhood_group,
                        latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s, %s)''',
                    (neighbourhood_id, listing_id, neighbourhood_name, neighbourhood_group,
                    latitude or None, longitude or None)
                )
                conn.commit()
                flash('Listing created successfully!', 'success')
                conn.close()
                return redirect(url_for('view_listings'))
            except IntegrityError as e:
                import traceback
                traceback.print_exc()
                flash(f"Error: {e.pgerror}", 'error')
                conn.rollback()
                conn.close()
                return redirect(url_for('add_listing'))

    # fetch hosts for dropdown
    cur.execute('SELECT host_id, host_name FROM Host ORDER BY host_name;')
    hosts = cur.fetchall()
    cur.close()
    conn.close()

    return render_template('add_listing.html', hosts=hosts)


# Route to update a particular listing.
@app.route('/update-listing', methods=['GET', 'POST'])
def update_listing():
    conn = get_db_connection()
    cur = conn.cursor()
    if request.method == 'POST':
        listing_id = request.form.get('listing_id', type=int)
        # Check existence
        cur.execute('SELECT 1 FROM Listing WHERE listing_id = %s;', (listing_id,))
        if not cur.fetchone():
            flash(f'No such listing exists: {listing_id}', 'error')
            cur.close()
            conn.close()
            return redirect(url_for('update_listing'))

        # Gather all optional fields
        fields = {
            'name': request.form.get('name'),
            'description': request.form.get('description'),
            'neighbourhood_overview': request.form.get('neighbourhood_overview'),
            'room_type': request.form.get('room_type'),
            'accommodates': request.form.get('accommodates', type=int),
            'bathrooms': request.form.get('bathrooms', type=float),
            'bedrooms': request.form.get('bedrooms', type=int),
            'price': request.form.get('price', type=float),
            'minimum_nights': request.form.get('minimum_nights', type=int)
        }
        nbhd = {
            'name': request.form.get('neighbourhood_name'),
            'neighbourhood_group': request.form.get('neighbourhood_group'),
            'latitude': request.form.get('latitude', type=float),
            'longitude': request.form.get('longitude', type=float)
        }

        # Build SET clauses for Listing table
        set_clauses = []
        params = []
        for col, val in fields.items():
            if val is not None and val != '':
                set_clauses.append(f"{col} = %s")
                params.append(val)
        if set_clauses:
            sql = f"UPDATE Listing SET {', '.join(set_clauses)} WHERE listing_id = %s;"
            params.append(listing_id)
            cur.execute(sql, params)

        # Build SET clauses for Neighbourhood table
        set_nb_clauses = []
        nb_params = []
        for col, val in nbhd.items():
            if val is not None and val != '':
                set_nb_clauses.append(f"{col} = %s")
                nb_params.append(val)
        if set_nb_clauses:
            sql_n = f"UPDATE Neighbourhood SET {', '.join(set_nb_clauses)} WHERE listing_id = %s;"
            nb_params.append(listing_id)
            cur.execute(sql_n, nb_params)

            # Update geopoint in listing if coordinates have been changed.
            if nbhd['latitude'] is not None and nbhd['longitude'] is not None:
                cur.execute(
                    """
                    UPDATE Listing
                    SET geopoint = ST_SetSRID(
                                    ST_MakePoint(%s, %s),
                                    4326
                                   )::geography
                     WHERE listing_id = %s;
                    """,
                    (
                        nbhd['longitude'],
                        nbhd['latitude'],
                        listing_id
                    )
                )

        conn.commit()
        flash(f'Update to listing {listing_id} successful', 'success')
        cur.close()
        conn.close()
        return redirect(url_for('view_listings'))

    # GET: render form
    cur.close()
    conn.close()
    return render_template('update_listing.html')

# Route to delete a particular listing.
@app.route('/delete-listing', methods=['GET', 'POST'])
def delete_listing():
    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == 'POST':
        listing_id = request.form.get('listing_id', type=int)
        
        # attempt to delete
        cur.execute('SELECT 1 FROM Listing WHERE listing_id = %s;', (listing_id,))
        if not cur.fetchone():
            flash(f'Listing ID {listing_id} not found.', 'error')
        else:
            cur.execute('DELETE FROM Listing WHERE listing_id = %s;', (listing_id,))
            conn.commit()
            flash(f'Listing ID {listing_id} deleted successfully.', 'success')
        cur.close()
        conn.close()
        return redirect(url_for('delete_listing'))

    # GET: render form
    cur.close()
    conn.close()
    return render_template('delete_listing.html')

# Route to remove the loaded sample data from the database.
@app.route('/delete-all')
def delete_all():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM listing;")
    cur.execute("DELETE FROM host;")
    conn.commit()
    cur.close()
    conn.close()

    return render_template('delete_all.html')

@app.route('/recommendations')
def recommendations_page():
    """
    Main recommendations page
    """
    return render_template('recommendations.html')

@app.route('/api/recommendations/search')
def search_listings():
    """
    Search for listings to get recommendations
    """
    query = request.args.get('q', '')
    if not query:
        return jsonify([])
    
    results = recommendation_engine.search_listings(query, limit=20)
    return jsonify(results)

@app.route('/api/recommendations/<int:listing_id>')
def get_recommendations(listing_id):
    """
    Get recommendations for a specific listing
    """
    max_results = request.args.get('limit', 10, type=int)
    similarity_threshold = request.args.get('threshold', 0.6, type=float)
    
    recommendations = recommendation_engine.get_listing_recommendations(
        listing_id, max_results, similarity_threshold
    )
    
    return jsonify(recommendations)

@app.route('/api/recommendations/listing/<int:listing_id>')
def get_listing_details(listing_id):
    """
    Get detailed information about a specific listing
    """
    listing = recommendation_engine.get_listing_details_for_comparison(listing_id)
    
    if listing:
        return jsonify(listing)
    else:
        return jsonify({'error': 'Listing not found'}), 404

@app.route('/api/recommendations/weights', methods=['POST'])
def update_similarity_weights():
    """
    Update similarity calculation weights
    """
    weights = request.get_json()
    
    if not weights:
        return jsonify({'error': 'No weights provided'}), 400
    
    success = recommendation_engine.update_similarity_weights(weights)
    
    if success:
        return jsonify({'message': 'Weights updated successfully'})
    else:
        return jsonify({'error': 'Invalid weights. Must sum to 1.0'}), 400

@app.route('/api/recommendations/weights', methods=['GET'])
def get_similarity_weights():
    """
    Get current similarity calculation weights
    """
    return jsonify(recommendation_engine.similarity_weights)

# Route to view host notifications (`Advanced Feature - Trigger-based)
@app.route('/notifications')
def view_notifications():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get filter parameters
    host_filter = request.args.get('host_id', type=int)
    notification_type = request.args.get('notification_type', type=str)
    status_filter = request.args.get('status', type=str)
    page = request.args.get('page', default=1, type=int)
    per_page = 20
    offset = (page - 1) * per_page
    
    # Build query with filters
    base_query = """
        SELECT 
            hn.notification_id,
            h.host_name,
            hn.notification_type,
            hn.message,
            l.name as related_listing_name,
            l.price as related_listing_price,
            n.name as neighbourhood,
            hn.created_at,
            hn.is_read
        FROM HostNotifications hn
        JOIN Host h ON hn.host_id = h.host_id
        LEFT JOIN Listing l ON hn.related_listing_id = l.listing_id
        LEFT JOIN Neighbourhood n ON l.listing_id = n.listing_id
    """
    count_query = """
        SELECT COUNT(*)
        FROM HostNotifications hn
        JOIN Host h ON hn.host_id = h.host_id
        LEFT JOIN Listing l ON hn.related_listing_id = l.listing_id
        LEFT JOIN Neighbourhood n ON l.listing_id = n.listing_id
    """
    where_clauses = []
    params = []
    count_params = []
    if host_filter:
        where_clauses.append("hn.host_id = %s")
        params.append(host_filter)
        count_params.append(host_filter)
    if notification_type:
        where_clauses.append("hn.notification_type = %s")
        params.append(notification_type)
        count_params.append(notification_type)
    if status_filter:
        if status_filter == 'read':
            where_clauses.append("hn.is_read = true")
            count_params.append(None)
        elif status_filter == 'unread':
            where_clauses.append("hn.is_read = false")
            count_params.append(None)
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)
        base_query += where_sql
        count_query += where_sql
    else:
        where_sql = ""
    base_query += " ORDER BY hn.created_at DESC LIMIT %s OFFSET %s"
    params.extend([per_page, offset])
    cur.execute(count_query, params[:len(params)-2] if len(params) > 2 else params)
    total_count = cur.fetchone()[0]
    total_pages = (total_count + per_page - 1) // per_page
    prev_page = max(1, page - 1)
    next_page = min(total_pages, page + 1)
    cur.execute(base_query, params)
    notifications = cur.fetchall()

    # Get hosts for filter dropdown
    cur.execute("SELECT host_id, host_name FROM Host ORDER BY host_name")
    hosts = cur.fetchall()
    # Format notifications for template
    formatted_notifications = []
    for notif in notifications:
        formatted_notifications.append({
            'notification_id': notif[0],
            'host_name': notif[1],
            'notification_type': notif[2],
            'message': notif[3],
            'related_listing_name': notif[4],
            'related_listing_price': float(notif[5]) if notif[5] else None,
            'neighbourhood': notif[6],
            'created_at': notif[7],
            'is_read': notif[8],
            'status': 'Read' if notif[8] else 'Unread'
        })
    cur.close()
    conn.close()
    return render_template('notifications.html', 
        notifications=formatted_notifications,
        hosts=hosts,
        current_filters={
            'host_id': host_filter,
            'notification_type': notification_type,
            'status': status_filter
        },
        page=page,
        total_pages=total_pages,
        prev_page=prev_page,
        next_page=next_page
    )

# Route to mark notification as read
@app.route('/mark-notification-read', methods=['POST'])
def mark_notification_read():
    conn = get_db_connection()
    cur = conn.cursor()
    
    notification_id = request.form.get('notification_id', type=int)
    
    cur.execute("UPDATE HostNotifications SET is_read = true WHERE notification_id = %s", (notification_id,))
    conn.commit()
    
    cur.close()
    conn.close()
    
    flash('Notification marked as read!', 'success')
    return redirect(url_for('view_notifications'))

# Route to view referral network analysis (Advanced Feature - Recursive Query)
@app.route('/referral-network')
def referral_network():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get filter parameters
    root_host_id = request.args.get('root_host_id', type=int)
    max_depth = 5  # Fixed depth for simplicity
    
    # Get all hosts in production order (by host_id)
    cur.execute("SELECT host_id, host_name FROM Host ORDER BY host_id")
    all_hosts = cur.fetchall()
    
    network_data = []
    network_summary = {}
    
    if root_host_id:
        # First, get total network revenue for percentage calculations
        total_revenue_query = """
        WITH RECURSIVE referral_network AS (
            SELECT h.host_id, h.host_name, h.referred_by, 0 as network_level
            FROM Host h WHERE h.host_id = %s
            UNION ALL
            SELECT h.host_id, h.host_name, h.referred_by, rn.network_level + 1
            FROM Host h
            JOIN referral_network rn ON h.referred_by = rn.host_id
            WHERE rn.network_level < %s
        )
        SELECT COALESCE(SUM(l.price * 30), 0) as total_network_revenue
        FROM referral_network rn
        JOIN Host h ON rn.host_id = h.host_id
        LEFT JOIN Listing l ON h.host_id = l.host_id;
        """
        
        cur.execute(total_revenue_query, (root_host_id, max_depth))
        total_network_revenue = cur.fetchone()[0]
        
        # Recursive query to get referral network with revenue calculations (fixing aggregation)
        recursive_query = """
        WITH RECURSIVE referral_network AS (
            -- Base case: Find the root host
            SELECT 
                h.host_id,
                h.host_name,
                h.referred_by,
                h.host_since,
                h.is_superhost,
                h.host_listings_count,
                0 as network_level,
                CAST(h.host_name AS VARCHAR(500)) as referral_path
            FROM Host h
            WHERE h.host_id = %s
            
            UNION ALL
            
            -- Recursive case: Find all hosts referred by previous level
            SELECT 
                h.host_id,
                h.host_name,
                h.referred_by,
                h.host_since,
                h.is_superhost,
                h.host_listings_count,
                rn.network_level + 1,
                CAST(rn.referral_path || ' -> ' || h.host_name AS VARCHAR(500))
            FROM Host h
            JOIN referral_network rn ON h.referred_by = rn.host_id
            WHERE rn.network_level < %s
        )
        SELECT 
            rn.network_level,
            rn.host_id,
            rn.host_name,
            rn.referral_path,
            rn.host_since,
            rn.is_superhost,
            rn.host_listings_count,
            COALESCE(AVG(l.price), 0) as avg_listing_price,
            COUNT(DISTINCT l.listing_id) as total_listings,
            COALESCE(SUM(l.price * 30), 0) as individual_monthly_revenue,
            CASE 
                WHEN %s > 0 THEN ROUND((COALESCE(SUM(l.price * 30), 0) / %s) * 100, 2)
                ELSE 0 
            END as revenue_percentage
        FROM referral_network rn
        LEFT JOIN Listing l ON rn.host_id = l.host_id
        GROUP BY rn.network_level, rn.host_id, rn.host_name, rn.referral_path, 
                 rn.host_since, rn.is_superhost, rn.host_listings_count
        ORDER BY rn.network_level, rn.host_id;
        """
        
        cur.execute(recursive_query, (root_host_id, max_depth, total_network_revenue, total_network_revenue))
        network_data = cur.fetchall()
        
        # Network performance summary
        if network_data:
            summary_query = """
            WITH RECURSIVE referral_network AS (
                SELECT h.host_id, h.host_name, h.referred_by, 0 as network_level
                FROM Host h WHERE h.host_id = %s
                UNION ALL
                SELECT h.host_id, h.host_name, h.referred_by, rn.network_level + 1
                FROM Host h
                JOIN referral_network rn ON h.referred_by = rn.host_id
                WHERE rn.network_level < %s
            )
            SELECT 
                COUNT(*) as total_network_hosts,
                MAX(network_level) as max_network_depth,
                COALESCE(AVG(r.rating), 0) as network_avg_rating,
                COUNT(DISTINCT CASE WHEN h.is_superhost THEN h.host_id END) as superhost_count,
                COUNT(DISTINCT l.listing_id) as total_network_listings
            FROM referral_network rn
            JOIN Host h ON rn.host_id = h.host_id
            LEFT JOIN Listing l ON h.host_id = l.host_id
            LEFT JOIN Review r ON l.listing_id = r.listing_id;
            """
            
            cur.execute(summary_query, (root_host_id, max_depth))
            result = cur.fetchone()
            if result:
                network_summary = {
                    'total_agents': len(network_data),
                    'max_depth': result[1],
                    'estimated_revenue': total_network_revenue,
                    'avg_rating': result[2],
                    'superhosts': result[3],
                    'total_listings': result[4]
                }
    
    cur.close()
    conn.close()
    
    return render_template('referral_network.html', 
        network_data=network_data,
        network_summary=network_summary,
        all_hosts=all_hosts,
        selected_host=root_host_id
    )

# Route to add or modify hosts with referral relationships
@app.route('/add-host-referral', methods=['GET', 'POST'])
@app.route('/add-host-referral/<int:host_id>', methods=['GET', 'POST'])
def add_host_referral(host_id=None):
    if request.method == 'POST':
        host_id = request.form.get('host_id')
        referred_by = request.form.get('referred_by')
        is_superhost = bool(request.form.get('is_superhost'))
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Only update existing host
            cur.execute("""
                UPDATE Host
                SET referred_by = %s, is_superhost = %s
                WHERE host_id = %s
            """, (referred_by if referred_by else None, is_superhost, host_id))
            conn.commit()
            flash('Agent successfully linked to brokerage firm!', 'success')
            return redirect(url_for('referral_network'))
        except Exception as e:
            conn.rollback()
            flash(f'Error linking agent: {str(e)}', 'error')
        finally:
            cur.close()
            conn.close()
    # GET request - show form
    # In add_host_referral, fetch up to 500 hosts for dropdowns
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT host_id, host_name FROM Host ORDER BY host_id LIMIT 500")
    existing_hosts = cur.fetchall()
    current_host = None
    if host_id:
        cur.execute("SELECT * FROM Host WHERE host_id = %s", (host_id,))
        current_host = cur.fetchone()
    cur.close()
    conn.close()
    return render_template('add_host_referral.html', 
        existing_hosts=existing_hosts, 
        current_host=current_host, 
        is_editing=host_id is not None
    )

# Route to view detailed information about a specific host and their listings
@app.route('/host-details/<int:host_id>')
def host_details(host_id):
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get host information
    cur.execute("""
        SELECT h.host_id, h.host_name, h.host_since, h.host_location, h.host_about,
               h.host_response_time, h.host_response_rate, h.host_acceptance_rate,
               h.is_superhost, h.host_listings_count, h.referred_by,
               r.host_name as referrer_name
        FROM Host h
        LEFT JOIN Host r ON h.referred_by = r.host_id
        WHERE h.host_id = %s
    """, (host_id,))
    
    host_info = cur.fetchone()
    if not host_info:
        flash(f'Host with ID {host_id} not found.', 'error')
        return redirect(url_for('referral_network'))
    
    # Get all listings for this host
    cur.execute("""
        SELECT l.listing_id, l.name, l.description, l.room_type, l.accommodates,
               l.price, l.minimum_nights, l.maximum_nights, l.instant_bookable,
               l.created_date, l.last_scraped,
               n.name as neighbourhood_name, n.neighbourhood_group,
               n.latitude, n.longitude,
               COALESCE(AVG(r.rating), 0) as avg_rating,
               COUNT(r.review_id) as review_count,
               COALESCE(SUM(r.number_of_reviews), 0) as total_reviews
        FROM Listing l
        LEFT JOIN Neighbourhood n ON l.listing_id = n.listing_id
        LEFT JOIN Review r ON l.listing_id = r.listing_id
        WHERE l.host_id = %s
        GROUP BY l.listing_id, l.name, l.description, l.room_type, l.accommodates,
                 l.price, l.minimum_nights, l.maximum_nights, l.instant_bookable,
                 l.created_date, l.last_scraped, n.name, n.neighbourhood_group,
                 n.latitude, n.longitude
        ORDER BY l.created_date DESC
    """, (host_id,))
    
    listings = cur.fetchall()
    
    # Get amenities for each listing
    listing_amenities = {}
    if listings:
        listing_ids = [listing[0] for listing in listings]
        placeholders = ','.join(['%s'] * len(listing_ids))
        cur.execute(f"""
            SELECT listing_id, amenity
            FROM ListingAmenity
            WHERE listing_id IN ({placeholders})
            ORDER BY listing_id, amenity
        """, listing_ids)
        
        amenities_result = cur.fetchall()
        for listing_id, amenity in amenities_result:
            if listing_id not in listing_amenities:
                listing_amenities[listing_id] = []
            listing_amenities[listing_id].append(amenity)
    
    # Calculate revenue and performance metrics
    total_daily_revenue = sum(float(listing[5]) for listing in listings)
    total_monthly_revenue = total_daily_revenue * 30
    avg_price = total_daily_revenue / len(listings) if listings else 0
    avg_rating = sum(float(listing[14]) for listing in listings) / len(listings) if listings else 0
    total_reviews = sum(int(listing[16]) for listing in listings)
    
    # Get network position if this host is part of a network
    network_info = None
    if host_info[10]:  # has referred_by
        cur.execute("""
            WITH RECURSIVE referral_network AS (
                -- Find the root of this host's network
                SELECT h.host_id, h.host_name, h.referred_by, 0 as level_from_root,
                       CAST(h.host_name AS VARCHAR(500)) as path_from_root
                FROM Host h 
                WHERE h.referred_by IS NULL 
                  AND h.host_id IN (
                      -- Find the root by following the chain up
                      WITH RECURSIVE find_root AS (
                          SELECT host_id, referred_by FROM Host WHERE host_id = %s
                          UNION ALL
                          SELECT h.host_id, h.referred_by 
                          FROM Host h 
                          JOIN find_root fr ON h.host_id = fr.referred_by
                      )
                      SELECT host_id FROM find_root WHERE referred_by IS NULL
                  )
                
                UNION ALL
                
                SELECT h.host_id, h.host_name, h.referred_by, rn.level_from_root + 1,
                       CAST(rn.path_from_root || ' → ' || h.host_name AS VARCHAR(500))
                FROM Host h
                JOIN referral_network rn ON h.referred_by = rn.host_id
                WHERE rn.level_from_root < 10
            )
            SELECT level_from_root, path_from_root
            FROM referral_network
            WHERE host_id = %s
        """, (host_id, host_id))
        
        network_result = cur.fetchone()
        if network_result:
            network_info = {
                'level': network_result[0],
                'path': network_result[1]
            }
    
    cur.close()
    conn.close()
    
    # Format host data for template
    host_data = {
        'host_id': host_info[0],
        'host_name': host_info[1],
        'host_since': host_info[2],
        'host_location': host_info[3],
        'host_about': host_info[4],
        'host_response_time': host_info[5],
        'host_response_rate': host_info[6],
        'host_acceptance_rate': host_info[7],
        'is_superhost': host_info[8],
        'host_listings_count': host_info[9],
        'referred_by': host_info[10],
        'referrer_name': host_info[11]
    }
    
    # Format listings data for template (simplified for the new table structure)
    listings_data = []
    for listing in listings:
        listings_data.append({
            'name': listing[1],
            'room_type': listing[3],
            'accommodates': listing[4],
            'price': float(listing[5])
        })
    
    performance_metrics = {
        'total_daily_revenue': total_daily_revenue,
        'total_monthly_revenue': total_monthly_revenue,
        'avg_price': avg_price,
        'avg_rating': avg_rating,
        'total_reviews': total_reviews,
        'total_listings': len(listings)
    }
    
    return render_template('host_details.html',
        host=host_data,
        listings=listings_data,
        performance=performance_metrics,
        network_info=network_info
    )

if __name__ == '__main__':
    # Ensure the schema is created before handling requests
    init_db()
    
    # Load production data if database is empty
    load_production_data_if_needed()
    
    # Initialize analytics views
    try:
        init_analytics_views()
        print("Analytics views initialized successfully!")
    except Exception as e:
        print(f"Warning: Failed to initialize analytics views: {e}")
    
    app.run(debug=True)