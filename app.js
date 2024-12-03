const express = require('express');
const mysql = require('mysql2');
const methodOverride = require('method-override');

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(methodOverride('_method'));

// MySQL connection pools for nodes
const poolNode1 = mysql.createPool({
    host: 'ccscloud.dlsu.edu.ph',
    port: 20752,
    user: 'grp29node1',
    password: 'grp29node1',
    database: 'mco2_database',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

const poolNode2 = mysql.createPool({
    host: 'ccscloud.dlsu.edu.ph',
    port: 20762,
    user: 'grp29node2',
    password: 'grp29node2',
    database: 'mco2_database_node2',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

const poolNode3 = mysql.createPool({
    host: 'ccscloud.dlsu.edu.ph',
    port: 20772,
    user: 'grp29node3',
    password: 'grp29node3',
    database: 'mco2_database_node3',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Helper function to replicate updates
function replicateUpdate(pool, query, params, nodeName) {
    pool.query(query, params, (err) => {
        if (err) {
            console.error(`Error replicating to ${nodeName}:`, err);
        }
    });
}

// Helper function to render games as HTML table
function renderTable(pool, nodeName, res) {
    pool.query('SELECT * FROM games', (err, results) => {
        if (err) {
            res.status(500).send('Database error');
            return;
        }

        let html = `
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>${nodeName} - Games</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 0;
                    background-color: #f4f4f9;
                }
                nav {
                    background-color: #007bff;
                    color: white;
                    padding: 1rem;
                    text-align: center;
                }
                nav a {
                    color: white;
                    text-decoration: none;
                    margin: 0 15px;
                    font-size: 1.2rem;
                }
                .container {
                    max-width: 900px;
                    margin: 20px auto;
                    background: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
                }
                h1 {
                    text-align: center;
                    color: #333;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 20px;
                }
                th, td {
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: left;
                }
                th {
                    background-color: #f4f4f9;
                }
                form {
                    display: flex;
                    gap: 10px;
                }
                input, button {
                    padding: 8px;
                    border-radius: 4px;
                    border: 1px solid #ccc;
                }
                button {
                    background-color: #007bff;
                    color: white;
                    border: none;
                    cursor: pointer;
                }
                button:hover {
                    background-color: #0056b3;
                }
            </style>
        </head>
        <body>
            <nav>
                <a href="/">Home</a>
                <a href="/node1/games">Node 1</a>
                <a href="/node2/games">Node 2</a>
                <a href="/node3/games">Node 3</a>
            </nav>
            <div class="container">
                <h1>${nodeName} - Games</h1>
                <form method="POST" action="/${nodeName.toLowerCase()}/games">
                    <input type="text" name="app_id" placeholder="App ID" required>
                    <input type="text" name="name" placeholder="Name" required>
                    <input type="date" name="release_date" required>
                    <input type="number" name="price" step="0.01" placeholder="Price" required>
                    <input type="text" name="developers" placeholder="Developers" required>
                    <input type="text" name="publishers" placeholder="Publishers" required>
                    <button type="submit">Add Game</button>
                </form>
                <table>
                    <thead>
                        <tr>
                            <th>App ID</th>
                            <th>Name</th>
                            <th>Release Date</th>
                            <th>Price</th>
                            <th>Developers</th>
                            <th>Publishers</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        results.forEach((game) => {
            html += `
                <tr>
                    <td>${game.app_id}</td>
                    <td>${game.name}</td>
                    <td>${new Date(game.release_date).toLocaleDateString()}</td>
                    <td>${isNaN(parseFloat(game.price)) ? 'N/A' : parseFloat(game.price).toFixed(2)}</td>
                    <td>${game.developers}</td>
                    <td>${game.publishers}</td>
                    <td class="actions">
                        <form method="POST" action="/${nodeName.toLowerCase()}/games/${game.app_id}?_method=PUT">
                            <input type="text" name="name" value="${game.name}" required>
                            <input type="date" name="release_date" value="${new Date(game.release_date).toISOString().split('T')[0]}" required>
                            <input type="number" name="price" value="${isNaN(parseFloat(game.price)) ? '' : parseFloat(game.price).toFixed(2)}" step="0.01" required>
                            <input type="text" name="developers" value="${game.developers}" required>
                            <input type="text" name="publishers" value="${game.publishers}" required>
                            <button type="submit">Update</button>
                        </form>
                        <form method="POST" action="/${nodeName.toLowerCase()}/games/${game.app_id}?_method=DELETE">
                            <button type="submit">Delete</button>
                        </form>
                    </td>
                </tr>
            `;
        });
        

        html += `
                    </tbody>
                </table>
            </div>
        </body>
        </html>
        `;

        res.send(html);
    });
}

// Define the setupCrudRoutes function
function setupCrudRoutes(pool, nodeName, partitionFilter) {
    const routeBase = `/${nodeName.toLowerCase()}`;

    // Read (List games)
    app.get(`${routeBase}/games`, (req, res) => {
        renderTable(pool, nodeName, res);
    });

    // Create
    app.post(`${routeBase}/games`, (req, res) => {
        const { app_id, name, release_date, price, developers, publishers } = req.body;

        // Validate duplicate App ID
        const checkQuery = 'SELECT app_id FROM games WHERE app_id = ?';
        pool.query(checkQuery, [app_id], (err, results) => {
            if (results.length > 0) {
                return res.status(400).send('Error: App ID already exists. Please use a unique App ID.');
            }

            // Validate partitioning rule
            if (partitionFilter && !partitionFilter(new Date(release_date))) {
                return res.status(400).send(`Game does not match ${nodeName} partitioning rules.`);
            }

            // Insert game into the current node
            const query = 'INSERT INTO games (app_id, name, release_date, price, developers, publishers) VALUES (?, ?, ?, ?, ?, ?)';
            pool.query(query, [app_id, name, release_date, price, developers, publishers], (err) => {
                if (err) {
                    res.status(500).send('Error adding game');
                    return;
                }

                // Replicate to Node 1 if not already Node 1
                if (nodeName !== 'Node1') {
                    replicateUpdate(
                        poolNode1,
                        query,
                        [app_id, name, release_date, price, developers, publishers],
                        'Node 1'
                    );
                }

                res.redirect(`${routeBase}/games`);
            });
        });
    });

    // Update
    app.put(`${routeBase}/games/:id`, (req, res) => {
        const { id } = req.params;
        const { name, release_date, price, developers, publishers } = req.body;
        const query = 'UPDATE games SET name = ?, release_date = ?, price = ?, developers = ?, publishers = ? WHERE app_id = ?';

        pool.query(query, [name, release_date, price, developers, publishers, id], (err) => {
            if (err) {
                res.status(500).send('Error updating game');
                return;
            }

            // Replicate to Node 1 if not already Node 1
            if (nodeName !== 'Node1') {
                replicateUpdate(poolNode1, query, [name, release_date, price, developers, publishers, id], 'Node 1');
            }

            // Replicate from Node 1 to Node 2 or Node 3
            if (nodeName === 'Node1') {
                const newReleaseDate = new Date(release_date);
                if (newReleaseDate.getFullYear() < 2010) {
                    replicateUpdate(poolNode2, query, [name, release_date, price, developers, publishers, id], 'Node 2');
                } else {
                    replicateUpdate(poolNode3, query, [name, release_date, price, developers, publishers, id], 'Node 3');
                }
            }

            res.redirect(`${routeBase}/games`);
        });
    });

    // Delete
    app.delete(`${routeBase}/games/:id`, (req, res) => {
        const { id } = req.params;
        const query = 'DELETE FROM games WHERE app_id = ?';

        pool.query(query, [id], (err) => {
            if (err) {
                res.status(500).send('Error deleting game');
            } else {
                res.redirect(`${routeBase}/games`);
            }
        });
    });
}

// Partitioning rules
const node2Partition = (releaseDate) => releaseDate.getFullYear() < 2010;
const node3Partition = (releaseDate) => releaseDate.getFullYear() >= 2010;

// Setup CRUD routes for each node
setupCrudRoutes(poolNode1, 'Node1');
setupCrudRoutes(poolNode2, 'Node2', node2Partition);
setupCrudRoutes(poolNode3, 'Node3', node3Partition);

// Root route
app.get('/', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Distributed Database System</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 0;
                    background-color: #f4f4f9;
                }
                nav {
                    background-color: #007bff;
                    color: white;
                    padding: 1rem;
                    text-align: center;
                }
                nav a {
                    color: white;
                    text-decoration: none;
                    margin: 0 15px;
                    font-size: 1.2rem;
                }
                .container {
                    max-width: 900px;
                    margin: 20px auto;
                    background: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
                    text-align: center;
                }
                h1 {
                    color: #333;
                    margin-bottom: 20px;
                }
                .node-links {
                    display: flex;
                    justify-content: space-around;
                    margin-top: 20px;
                }
                .node-links a {
                    display: block;
                    padding: 10px 20px;
                    border-radius: 5px;
                    background-color: #007bff;
                    color: white;
                    text-decoration: none;
                    font-weight: bold;
                }
                .node-links a:hover {
                    background-color: #0056b3;
                }
                @media (max-width: 768px) {
                    .node-links {
                        flex-direction: column;
                        gap: 10px;
                    }
                }
            </style>
        </head>
        <body>
            <nav>
                <a href="/">Home</a>
                <a href="/node1/games">Node 1</a>
                <a href="/node2/games">Node 2</a>
                <a href="/node3/games">Node 3</a>
            </nav>
            <div class="container">
                <h1>Welcome to the Distributed Database System</h1>
                <p>This system demonstrates a distributed database setup with three nodes:</p>
                <div class="node-links">
                    <a href="/node1/games">Node 1: Central Repository</a>
                    <a href="/node2/games">Node 2: Games Released Before 2010</a>
                    <a href="/node3/games">Node 3: Games Released After 2010</a>
                </div>
            </div>
        </body>
        </html>
    `);
});


// Start server
app.listen(3000, () => {
    console.log('Server is running on http://localhost:3000');
});
