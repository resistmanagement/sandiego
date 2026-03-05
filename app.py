#!/usr/bin/env python3
"""
San Diego Property Auction Web Viewer
Simple Flask app to browse and search auction properties
"""

import os
from flask import Flask, render_template, request, jsonify, send_from_directory
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/sandiego_auction")
PHOTOS_DIR = os.path.join(os.path.dirname(__file__), 'photos')


def get_db_connection():
    """Create a database connection"""
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)


@app.route('/')
def index():
    """Main page with property listing"""
    return render_template('index.html')


@app.route('/api/properties')
def get_properties():
    """API endpoint to fetch properties with optional search"""
    search = request.args.get('search', '').strip()
    status_filter = request.args.get('status', '').strip()
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Build query
    query = """
        SELECT 
            p.*,
            COUNT(DISTINCT i.id) as photo_count,
            COUNT(DISTINCT t.id) as tax_years,
            COUNT(DISTINCT s.id) as sale_count
        FROM auction_properties p
        LEFT JOIN property_images i ON p.id = i.auction_property_id AND i.image_type = 'photo'
        LEFT JOIN property_tax_history t ON p.id = t.auction_property_id
        LEFT JOIN property_sale_history s ON p.id = s.auction_property_id
        WHERE 1=1
    """
    params = []
    
    # Apply search filter - including all property type fields
    if search:
        query += """ AND (
            p.item_number ILIKE %s
            OR p.address ILIKE %s
            OR p.city ILIKE %s
            OR p.assessee ILIKE %s
            OR p.apn ILIKE %s
            OR p.property_type ILIKE %s
            OR p.redfin_property_type ILIKE %s
            OR p.use_type ILIKE %s
        )"""
        search_pattern = f"%{search}%"
        params.extend([search_pattern] * 8)
    
    # Apply status filter
    if status_filter:
        query += " AND p.status = %s"
        params.append(status_filter)
    
    query += """
        GROUP BY p.id
        ORDER BY p.item_number
    """
    
    cur.execute(query, params)
    properties = cur.fetchall()
    
    # Convert decimals and dates to strings for JSON
    result = []
    for prop in properties:
        prop_dict = dict(prop)
        for key, value in prop_dict.items():
            if value is not None:
                if hasattr(value, 'isoformat'):  # datetime/date
                    prop_dict[key] = value.isoformat()
                elif isinstance(value, (int, float)):
                    prop_dict[key] = value
                else:
                    prop_dict[key] = str(value)
        result.append(prop_dict)
    
    cur.close()
    conn.close()
    
    return jsonify(result)


@app.route('/api/property/<int:property_id>')
def get_property_details(property_id):
    """Get detailed information for a single property"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get property
    cur.execute("SELECT * FROM auction_properties WHERE id = %s", (property_id,))
    property_data = cur.fetchone()
    
    if not property_data:
        return jsonify({'error': 'Property not found'}), 404
    
    # Get images - check if they're stored as local paths or URLs
    cur.execute(
        "SELECT * FROM property_images WHERE auction_property_id = %s ORDER BY sort_order",
        (property_id,)
    )
    images = cur.fetchall()
    
    # Convert image URLs to proper format
    formatted_images = []
    item_number = property_data['item_number']
    for img in images:
        img_dict = dict(img)
        url = img_dict['image_url']
        
        # If it's already a full URL, keep it; otherwise, construct local path
        if not url.startswith('http'):
            # Assume it's a filename in photos/{item_number}/ directory
            img_dict['image_url'] = f"{item_number}/{url}"
        else:
            # It's a remote URL - we'll need to handle this differently
            # For now, check if we have a local copy
            filename = url.split('/')[-1]
            local_path = f"{item_number}/{filename}"
            img_dict['image_url'] = local_path
        
        formatted_images.append(img_dict)
    
    # Get tax history
    cur.execute(
        "SELECT * FROM property_tax_history WHERE auction_property_id = %s ORDER BY tax_year DESC",
        (property_id,)
    )
    tax_history = cur.fetchall()
    
    # Get sale history
    cur.execute(
        "SELECT * FROM property_sale_history WHERE auction_property_id = %s ORDER BY sale_date DESC",
        (property_id,)
    )
    sale_history = cur.fetchall()
    
    cur.close()
    conn.close()
    
    # Format response
    def serialize(obj):
        result = dict(obj) if obj else {}
        for key, value in result.items():
            if value is not None:
                if hasattr(value, 'isoformat'):
                    result[key] = value.isoformat()
                else:
                    result[key] = str(value)
        return result
    
    return jsonify({
        'property': serialize(property_data),
        'images': formatted_images,
        'tax_history': [serialize(tax) for tax in tax_history],
        'sale_history': [serialize(sale) for sale in sale_history]
    })


@app.route('/api/stats')
def get_stats():
    """Get summary statistics"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            status,
            COUNT(*) as count,
            MIN(opening_bid) as min_bid,
            MAX(opening_bid) as max_bid,
            AVG(opening_bid) as avg_bid
        FROM auction_properties
        WHERE opening_bid IS NOT NULL
        GROUP BY status
        ORDER BY count DESC
    """)
    stats = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return jsonify([dict(row) for row in stats])


@app.route('/photos/<path:filename>')
def serve_photo(filename):
    """Serve photo files from the photos directory"""
    return send_from_directory(PHOTOS_DIR, filename)


@app.route('/api/property/<int:property_id>/rating', methods=['POST'])
def update_rating(property_id):
    """Update the rating for a property"""
    try:
        data = request.get_json()
        rating = data.get('rating')  # 'thumbs_up', 'thumbs_down', or None to clear
        
        if rating not in ['thumbs_up', 'thumbs_down', None]:
            return jsonify({'error': 'Invalid rating value'}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            "UPDATE auction_properties SET user_rating = %s WHERE id = %s",
            (rating, property_id)
        )
        conn.commit()
        
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'rating': rating})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
