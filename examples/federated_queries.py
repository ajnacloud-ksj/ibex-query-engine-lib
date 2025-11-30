"""
Federated Query Examples

Demonstrates how to query across multiple data sources using ibexdb
with BOTH structured requests (no SQL) AND SQL queries.
"""

from ibexdb import IbexDB, FederatedQueryEngine, create_federated_engine
from datetime import datetime

# ============================================================================
# Example 1: Simple IbexDB with Structured API (No SQL!)
# ============================================================================

def example_structured_api():
    """Query IbexDB using structured requests - no SQL knowledge needed!"""
    
    print("\n" + "="*60)
    print("Example 1: Structured API (No SQL)")
    print("="*60)
    
    db = IbexDB.from_env()
    
    # Structured query - just pass parameters!
    results = db.query(
        table="users",
        projection=["id", "name", "email", "status"],
        filters=[
            {"field": "age", "operator": "gte", "value": 18},
            {"field": "status", "operator": "eq", "value": "active"}
        ],
        aggregations=[
            {"field": None, "function": "count", "alias": "total_users"},
            {"field": "age", "function": "avg", "alias": "avg_age"}
        ],
        group_by=["status"],
        sort=[{"field": "total_users", "order": "desc"}],
        limit=100
    )
    
    if results.success:
        print(f"✓ Query succeeded!")
        print(f"  Records: {len(results.data.records)}")
        for record in results.data.records[:5]:
            print(f"  {record}")
    else:
        print(f"✗ Query failed: {results.error}")


# ============================================================================
# Example 2: IbexDB with SQL API
# ============================================================================

def example_sql_api():
    """Query IbexDB using SQL - for advanced users"""
    
    print("\n" + "="*60)
    print("Example 2: SQL API on Single Source")
    print("="*60)
    
    db = IbexDB.from_env()
    
    # Write SQL directly!
    results = db.execute_sql("""
        SELECT 
            status,
            COUNT(*) as total_users,
            AVG(age) as avg_age,
            MIN(age) as min_age,
            MAX(age) as max_age
        FROM users
        WHERE age >= 18
        GROUP BY status
        ORDER BY total_users DESC
        LIMIT 10
    """)
    
    print(f"✓ SQL query executed!")
    print(f"  Records: {len(results)}")
    for record in results[:5]:
        print(f"  {record}")


# ============================================================================
# Example 3: Federated Query with Structured API
# ============================================================================

def example_federated_structured():
    """Query across multiple sources with structured API"""
    
    print("\n" + "="*60)
    print("Example 3: Federated Query (Structured)")
    print("="*60)
    
    # Create federated engine
    fed = FederatedQueryEngine()
    
    # Add IbexDB source
    fed.add_source("ibexdb_prod", "ibexdb", {
        "tenant_id": "my_company",
        "namespace": "production"
    })
    
    # Add PostgreSQL source
    fed.add_source("postgres_analytics", "postgres", {
        "host": "localhost",
        "port": 5432,
        "database": "analytics",
        "user": "user",
        "password": "password"
    })
    
    # Add MySQL source
    fed.add_source("mysql_orders", "mysql", {
        "host": "localhost",
        "port": 3306,
        "database": "ecommerce",
        "user": "user",
        "password": "password"
    })
    
    # Structured federated query - NO SQL!
    result = fed.query(
        sources={
            "u": {"source": "postgres_analytics", "table": "users"},
            "e": {"source": "ibexdb_prod", "table": "events"},
            "o": {"source": "mysql_orders", "table": "orders"}
        },
        projection=[
            "u.name",
            "u.email",
            "COUNT(e.event_id) as event_count",
            "SUM(o.total) as total_revenue"
        ],
        join={
            "type": "left",
            "left": "u",
            "right": "e",
            "on": {"u.id": "e.user_id"}
        },
        filters=[
            {"source": "u", "field": "status", "operator": "eq", "value": "active"},
            {"source": "e", "field": "event_date", "operator": "gte", "value": "2024-01-01"}
        ],
        group_by=["u.name", "u.email"],
        sort=[{"field": "total_revenue", "order": "desc"}],
        limit=100
    )
    
    print(f"✓ Federated query executed!")
    print(f"  Records: {len(result)}")
    print(f"  Columns: {result.columns}")
    
    fed.close()


# ============================================================================
# Example 4: Federated Query with SQL
# ============================================================================

def example_federated_sql():
    """Query across multiple sources with raw SQL"""
    
    print("\n" + "="*60)
    print("Example 4: Federated Query (SQL)")
    print("="*60)
    
    # Create federated engine
    fed = create_federated_engine({
        "ibexdb": {
            "type": "ibexdb",
            "config": {
                "tenant_id": "my_company",
                "namespace": "production"
            }
        },
        "postgres": {
            "type": "postgres",
            "config": {
                "host": "localhost",
                "database": "analytics",
                "user": "user",
                "password": "password"
            }
        },
        "mysql": {
            "type": "mysql",
            "config": {
                "host": "localhost",
                "database": "ecommerce",
                "user": "user",
                "password": "password"
            }
        }
    })
    
    # Write SQL that spans multiple sources!
    result = fed.execute_sql("""
        SELECT 
            u.name,
            u.email,
            u.country,
            COUNT(DISTINCT e.event_id) as event_count,
            COUNT(DISTINCT o.order_id) as order_count,
            SUM(o.total) as total_revenue,
            AVG(o.total) as avg_order_value
        FROM postgres.users u
        LEFT JOIN ibexdb.events e 
            ON u.id = e.user_id 
            AND e.event_date >= '2024-01-01'
        LEFT JOIN mysql.orders o 
            ON u.id = o.user_id 
            AND o.order_date >= '2024-01-01'
        WHERE u.status = 'active'
        GROUP BY u.name, u.email, u.country
        HAVING total_revenue > 1000
        ORDER BY total_revenue DESC
        LIMIT 100
    """)
    
    print(f"✓ Federated SQL executed!")
    print(f"  Records: {len(result)}")
    for row in result[:5].to_dicts():
        print(f"  {row['name']}: ${row['total_revenue']:.2f}")
    
    fed.close()


# ============================================================================
# Example 5: Real-World Use Case - Customer 360
# ============================================================================

def example_customer_360():
    """Build a customer 360 view across multiple sources"""
    
    print("\n" + "="*60)
    print("Example 5: Customer 360 View (Multi-Source)")
    print("="*60)
    
    fed = FederatedQueryEngine()
    
    # Data sources
    fed.add_source("crm", "postgres", {
        "host": "crm-server",
        "database": "crm",
        "user": "app",
        "password": "password"
    })
    
    fed.add_source("analytics", "ibexdb", {
        "tenant_id": "company",
        "namespace": "analytics"
    })
    
    fed.add_source("billing", "mysql", {
        "host": "billing-server",
        "database": "billing",
        "user": "app",
        "password": "password"
    })
    
    # Customer 360 query
    customer_360 = fed.execute_sql("""
        WITH customer_profile AS (
            SELECT 
                c.customer_id,
                c.name,
                c.email,
                c.segment,
                c.signup_date
            FROM crm.customers c
            WHERE c.status = 'active'
        ),
        customer_behavior AS (
            SELECT 
                e.customer_id,
                COUNT(DISTINCT e.session_id) as sessions,
                COUNT(e.event_id) as total_events,
                SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) as purchases
            FROM analytics.events e
            WHERE e.event_date >= CURRENT_DATE - INTERVAL 90 DAYS
            GROUP BY e.customer_id
        ),
        customer_revenue AS (
            SELECT 
                i.customer_id,
                COUNT(i.invoice_id) as invoice_count,
                SUM(i.amount) as total_revenue,
                AVG(i.amount) as avg_invoice
            FROM billing.invoices i
            WHERE i.invoice_date >= CURRENT_DATE - INTERVAL 90 DAYS
            AND i.status = 'paid'
            GROUP BY i.customer_id
        )
        SELECT 
            p.*,
            COALESCE(b.sessions, 0) as sessions_90d,
            COALESCE(b.total_events, 0) as events_90d,
            COALESCE(b.purchases, 0) as purchases_90d,
            COALESCE(r.invoice_count, 0) as invoices_90d,
            COALESCE(r.total_revenue, 0) as revenue_90d,
            COALESCE(r.avg_invoice, 0) as avg_invoice_90d,
            CASE 
                WHEN r.total_revenue >= 10000 THEN 'VIP'
                WHEN r.total_revenue >= 1000 THEN 'Premium'
                ELSE 'Standard'
            END as tier
        FROM customer_profile p
        LEFT JOIN customer_behavior b ON p.customer_id = b.customer_id
        LEFT JOIN customer_revenue r ON p.customer_id = r.customer_id
        ORDER BY revenue_90d DESC
        LIMIT 1000
    """)
    
    print(f"✓ Customer 360 view created!")
    print(f"  Total customers: {len(customer_360)}")
    print(f"\nSample customers:")
    for customer in customer_360[:3].to_dicts():
        print(f"  {customer['name']}")
        print(f"    Segment: {customer['segment']}, Tier: {customer['tier']}")
        print(f"    Revenue (90d): ${customer['revenue_90d']:.2f}")
        print(f"    Sessions (90d): {customer['sessions_90d']}")
        print()
    
    fed.close()


# ============================================================================
# Example 6: Combining Structured and SQL
# ============================================================================

def example_hybrid_approach():
    """Use both structured API and SQL in the same workflow"""
    
    print("\n" + "="*60)
    print("Example 6: Hybrid Approach (Structured + SQL)")
    print("="*60)
    
    # Step 1: Use structured API to get user list from IbexDB
    db = IbexDB.from_env()
    
    active_users = db.query(
        table="users",
        projection=["id", "name", "email"],
        filters=[
            {"field": "status", "operator": "eq", "value": "active"},
            {"field": "last_login", "operator": "gte", "value": "2024-01-01"}
        ]
    )
    
    print(f"✓ Step 1: Found {len(active_users.data.records)} active users")
    
    # Step 2: Use SQL for complex multi-source analytics
    fed = FederatedQueryEngine()
    fed.add_source("ibexdb", "ibexdb", {"tenant_id": "company", "namespace": "prod"})
    fed.add_source("postgres", "postgres", {
        "host": "localhost",
        "database": "analytics",
        "user": "user",
        "password": "password"
    })
    
    user_ids = [u['id'] for u in active_users.data.records[:100]]
    ids_str = ",".join(map(str, user_ids))
    
    detailed_analytics = fed.execute_sql(f"""
        SELECT 
            u.id,
            u.name,
            COUNT(e.event_id) as events,
            SUM(e.value) as total_value
        FROM postgres.users u
        LEFT JOIN ibexdb.events e ON u.id = e.user_id
        WHERE u.id IN ({ids_str})
        GROUP BY u.id, u.name
        ORDER BY total_value DESC
    """)
    
    print(f"✓ Step 2: Analyzed {len(detailed_analytics)} users")
    print(f"\nTop users by value:")
    for user in detailed_analytics[:5].to_dicts():
        print(f"  {user['name']}: {user['events']} events, ${user['total_value']:.2f}")
    
    fed.close()


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("="*60)
    print("IbexDB Federated Query Examples")
    print("Structured API (No SQL) + SQL API + Federation")
    print("="*60)
    
    try:
        # Example 1: Structured API
        example_structured_api()
        
        # Example 2: SQL API
        example_sql_api()
        
        # Example 3: Federated Structured
        example_federated_structured()
        
        # Example 4: Federated SQL
        example_federated_sql()
        
        # Example 5: Customer 360
        example_customer_360()
        
        # Example 6: Hybrid Approach
        example_hybrid_approach()
        
        print("\n" + "="*60)
        print("✓ All examples completed!")
        print("="*60)
        print("\nKey Takeaways:")
        print("1. ✓ Structured API - No SQL knowledge needed")
        print("2. ✓ SQL API - For advanced users and complex queries")
        print("3. ✓ Federated queries - Query across multiple sources")
        print("4. ✓ Hybrid approach - Use both as needed")
        print("5. ✓ Type-safe with Pydantic validation")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

