#!/usr/bin/env python
"""
Script to initialize the database on Render.
This runs once during the build process.
"""

import os
import sys
from app import init_db, get_db_connection
from data_ingestion import load_production_data_if_needed
from analytics import init_analytics_views

def check_if_initialized():
    """Check if database is already initialized"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'listing'
            );
        """)
        table_exists = cur.fetchone()[0]
        cur.close()
        conn.close()
        return table_exists
    except Exception as e:
        print(f"Error checking database: {e}")
        return False

def initialize_database():
    """Initialize database schema, data, and analytics"""
    try:
        if check_if_initialized():
            print("✓ Database already initialized. Skipping.")
            return True
        
        print("Initializing database schema...")
        init_db()
        print("✓ Schema created successfully!")
        
        print("Loading production data...")
        load_production_data_if_needed()
        print("✓ Production data loaded!")
        
        print("Initializing analytics views...")
        init_analytics_views()
        print("✓ Analytics views created!")
        
        print("\n🎉 Database initialization complete!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error during initialization: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = initialize_database()
    sys.exit(0 if success else 1)
