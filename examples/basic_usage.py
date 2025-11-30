"""
Basic IbexDB Usage Examples

This demonstrates the core functionality of IbexDB library.
"""

from ibexdb import IbexDB
from datetime import datetime

# ============================================================================
# Example 1: Initialize Client
# ============================================================================

def example_initialize():
    """Initialize IbexDB client"""
    
    # From environment variables
    db = IbexDB.from_env()
    print(f"✓ Initialized: {db}")
    
    # Or with explicit parameters
    db = IbexDB(
        tenant_id="my_app",
        namespace="production"
    )
    print(f"✓ Initialized: {db}")
    
    return db


# ============================================================================
# Example 2: Create Table and Write Data
# ============================================================================

def example_create_and_write():
    """Create a table and write data"""
    db = IbexDB.from_env()
    
    # Create table with schema
    response = db.create_table(
        table="users",
        schema={
            "fields": {
                "id": {"type": "integer", "required": True},
                "name": {"type": "string", "required": True},
                "email": {"type": "string"},
                "age": {"type": "integer"},
                "status": {"type": "string"},
                "created_at": {"type": "timestamp"}
            },
            "primary_key": ["id"]
        }
    )
    
    if response.success:
        print(f"✓ Table created: {response.data.table_created}")
    
    # Write data
    response = db.write(
        table="users",
        records=[
            {
                "id": 1,
                "name": "Alice Smith",
                "email": "alice@example.com",
                "age": 30,
                "status": "active",
                "created_at": datetime.now()
            },
            {
                "id": 2,
                "name": "Bob Johnson",
                "email": "bob@example.com",
                "age": 25,
                "status": "active",
                "created_at": datetime.now()
            },
            {
                "id": 3,
                "name": "Charlie Brown",
                "email": "charlie@example.com",
                "age": 35,
                "status": "inactive",
                "created_at": datetime.now()
            }
        ]
    )
    
    if response.success:
        print(f"✓ Wrote {response.data.records_written} records")


# ============================================================================
# Example 3: Query Data
# ============================================================================

def example_query():
    """Query data with filters and sorting"""
    db = IbexDB.from_env()
    
    # Simple query
    response = db.query(
        table="users",
        projection=["id", "name", "email"],
        limit=10
    )
    
    if response.success:
        print(f"✓ Query returned {len(response.data.records)} records")
        for record in response.data.records:
            print(f"  - {record['name']} ({record['email']})")
    
    # Query with filters
    response = db.query(
        table="users",
        filters=[
            {"field": "age", "operator": "gte", "value": 30},
            {"field": "status", "operator": "eq", "value": "active"}
        ],
        sort=[{"field": "name", "order": "asc"}]
    )
    
    if response.success:
        print(f"✓ Filtered query returned {len(response.data.records)} records")


# ============================================================================
# Example 4: Update and Delete
# ============================================================================

def example_update_delete():
    """Update and delete records"""
    db = IbexDB.from_env()
    
    # Update records
    response = db.update(
        table="users",
        updates={"status": "verified"},
        filters=[
            {"field": "age", "operator": "gte", "value": 18}
        ]
    )
    
    if response.success:
        print(f"✓ Updated {response.data.records_updated} records")
    
    # Soft delete
    response = db.delete(
        table="users",
        filters=[
            {"field": "status", "operator": "eq", "value": "inactive"}
        ],
        mode="soft"
    )
    
    if response.success:
        print(f"✓ Deleted {response.data.records_deleted} records")


# ============================================================================
# Example 5: Aggregations
# ============================================================================

def example_aggregations():
    """Perform aggregations and grouping"""
    db = IbexDB.from_env()
    
    response = db.query(
        table="users",
        projection=["status"],
        aggregations=[
            {"field": None, "function": "count", "alias": "user_count"},
            {"field": "age", "function": "avg", "alias": "avg_age"}
        ],
        group_by=["status"]
    )
    
    if response.success:
        print(f"✓ Aggregation results:")
        for record in response.data.records:
            print(f"  - Status {record['status']}: {record['user_count']} users, avg age {record['avg_age']:.1f}")


# ============================================================================
# Example 6: Complex Data Types
# ============================================================================

def example_complex_types():
    """Work with arrays, structs, and maps"""
    db = IbexDB.from_env()
    
    # Create table with complex types
    db.create_table(
        table="products",
        schema={
            "fields": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "tags": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "metadata": {
                    "type": "map",
                    "key_type": "string",
                    "value_type": {"type": "string"}
                },
                "warehouse": {
                    "type": "struct",
                    "fields": {
                        "location": {"type": "string"},
                        "quantity": {"type": "integer"}
                    }
                }
            }
        }
    )
    
    # Write complex data
    db.write(
        table="products",
        records=[{
            "id": 1,
            "name": "Widget",
            "tags": ["electronics", "gadget", "popular"],
            "metadata": {"color": "blue", "size": "medium"},
            "warehouse": {
                "location": "Building A",
                "quantity": 150
            }
        }]
    )
    
    print("✓ Created table with complex types")


# ============================================================================
# Example 7: Time Travel
# ============================================================================

def example_time_travel():
    """Query historical data"""
    from datetime import timedelta
    
    db = IbexDB.from_env()
    
    # Query data as it was 7 days ago
    past_time = datetime.now() - timedelta(days=7)
    response = db.query(
        table="users",
        as_of=past_time
    )
    
    if response.success:
        print(f"✓ Historical query (7 days ago): {len(response.data.records)} records")


# ============================================================================
# Example 8: Maintenance
# ============================================================================

def example_maintenance():
    """Compact tables and manage snapshots"""
    db = IbexDB.from_env()
    
    # Compact table
    response = db.compact(
        table="users",
        force=False,
        target_file_size_mb=128,
        expire_snapshots=True
    )
    
    if response.success:
        if response.data.compacted:
            stats = response.data.stats
            print(f"✓ Compaction completed:")
            print(f"  Files: {stats.files_before} → {stats.files_after}")
            print(f"  Size: {stats.bytes_before / 1024**2:.2f} MB → {stats.bytes_after / 1024**2:.2f} MB")
            print(f"  Saved: {stats.bytes_saved / 1024**2:.2f} MB")
        else:
            print(f"✓ Compaction skipped: {response.data.reason}")


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("="*60)
    print("IbexDB Usage Examples")
    print("="*60)
    
    try:
        print("\n1. Initialize Client")
        example_initialize()
        
        print("\n2. Create Table and Write Data")
        example_create_and_write()
        
        print("\n3. Query Data")
        example_query()
        
        print("\n4. Update and Delete")
        example_update_delete()
        
        print("\n5. Aggregations")
        example_aggregations()
        
        print("\n6. Complex Data Types")
        example_complex_types()
        
        print("\n7. Time Travel")
        example_time_travel()
        
        print("\n8. Maintenance")
        example_maintenance()
        
        print("\n" + "="*60)
        print("✓ All examples completed successfully!")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

