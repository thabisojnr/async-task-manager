import aiosqlite
import asyncio
from typing import List, Dict, Any, Optional

class Database:
    def __init__(self, db_path: str = 'tasks.db'):
        self.db_path = db_path
        
    async def connect(self) -> None:
        """Establish a connection to the database and create tables."""
        self.connection = await aiosqlite.connect(self.db_path)
        await self.create_tables()
        print("Database connection established and tables verified.")
        
    async def create_tables(self) -> None:
        """Create the Users and Tasks tables if they don't exist."""
        create_users_table = """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL
        );
        """
        
        create_tasks_table = """
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            status TEXT DEFAULT 'pending',
            user_id INTEGER NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """
        
        await self.connection.execute(create_users_table)
        await self.connection.execute(create_tasks_table)
        await self.connection.commit()
        print("Tables 'users' and 'tasks' are ready.")

    async def create_user(self, username: str, email: str) -> None:
        """Insert a new user into the database."""
        query = "INSERT INTO users (username, email) VALUES (?, ?)"
        try:
            await self.connection.execute(query, (username, email))
            await self.connection.commit()
            print(f"User '{username}' created successfully.")
        except aiosqlite.IntegrityError:
            print(f"Error: Username '{username}' or email '{email}' already exists.")

    async def create_task(self, title: str, user_id: int, description: Optional[str] = None) -> None:
        """Insert a new task for a specific user into the database."""
        query = "INSERT INTO tasks (title, description, user_id) VALUES (?, ?, ?)"
        await self.connection.execute(query, (title, description, user_id))
        await self.connection.commit()
        print(f"Task '{title}' created for user_id {user_id}.")

    async def get_user_tasks(self, user_id: int) -> List[Dict]:
        """
        Retrieve all tasks for a specific user.
        
        Time Complexity: O(n) where n is the number of tasks for the user.
        - The JOIN operation is efficient with proper indexing
        - Linear scan through the result set
        
        Space Complexity: O(n) 
        - We store all results in memory in the 'tasks' list
        - Each task is stored as a dictionary with fixed number of keys
        """
        query = """
        SELECT tasks.id, tasks.title, tasks.description, tasks.status, users.username
        FROM tasks
        JOIN users ON tasks.user_id = users.id
        WHERE users.id = ?
        """
        cursor = await self.connection.execute(query, (user_id,))
        rows = await cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        tasks = [dict(zip(columns, row)) for row in rows]
        return tasks

    async def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Get a user by their ID. Returns None if not found."""
        try:
            query = "SELECT * FROM users WHERE id = ?"
            cursor = await self.connection.execute(query, (user_id,))
            user = await cursor.fetchone()
            if user:
                columns = [column[0] for column in cursor.description]
                return dict(zip(columns, user))
            return None
        except aiosqlite.Error as e:
            print(f"Database error when fetching user: {e}")
            return None

    async def close(self) -> None:
        """Properly close the database connection."""
        if hasattr(self, 'connection'):
            await self.connection.close()
            print("Database connection closed properly.")

class TaskRepository:
    """A repository to handle all task-related operations."""
    
    def __init__(self, db: Database):
        self.db = db
    
    async def create_user_and_tasks(self, username: str, email: str, tasks: List[str]) -> None:
        """
        A business logic method: Create a user and their tasks in one transaction.
        Demonstrates atomic operations - all succeed or all fail.
        """
        try:
            # Create user
            await self.db.create_user(username, email)
            
            # For a real application, we would get the user ID properly
            # For this example, we'll assume the new user has ID 1
            user_id = 1
            
            # Create all tasks for this user
            for task_title in tasks:
                await self.db.create_task(task_title, user_id)
                
            print(f"Successfully created user '{username}' with {len(tasks)} tasks.")
            
        except Exception as e:
            print(f"Failed to create user and tasks: {e}")
            # In a real application, we would rollback the transaction here

async def main():
    print("=== Async Task Manager ===")
    print("Initializing database...")
    
    # Initialize database and repository
    db = Database()
    task_repo = TaskRepository(db)
    
    try:
        await db.connect()
        
        # Demo: Create user with multiple tasks using our repository
        print("\n--- Demo: Creating user with tasks ---")
        demo_tasks = ["Study Python", "Prepare for interview", "Build async project"]
        await task_repo.create_user_and_tasks("interview_user", "interview@example.com", demo_tasks)
        
        # Demo: Get user data with error handling
        print("\n--- Demo: Fetching user data ---")
        user_data = await db.get_user_by_id(1)
        if user_data:
            print(f"Found user: {user_data['username']} ({user_data['email']})")
        else:
            print("User not found")
        
        # Demo: Get all tasks with complexity analysis
        print("\n--- Demo: Fetching all tasks ---")
        all_tasks = await db.get_user_tasks(1)
        print(f"Found {len(all_tasks)} tasks:")
        for task in all_tasks:
            print(f"  - {task['title']} [{task['status']}]")
        
        # Demo: Error handling - try to create duplicate user
        print("\n--- Demo: Error handling ---")
        await db.create_user("interview_user", "duplicate@example.com")
        
    except Exception as e:
        print(f"Application error: {e}")
    finally:
        # Ensure proper cleanup even if errors occur
        await db.close()
    
    print("\n=== Application finished ===")

if __name__ == "__main__":
    asyncio.run(main())