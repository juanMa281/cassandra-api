const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { Client } = require('cassandra-driver');

const app = express();
const port = 3000;

const client = new Client({
  contactPoints: ['192.168.18.173', '192.168.18.174'],
  localDataCenter: 'dc1',
  keyspace: 'basededatos',
});

// Conectar al iniciar el servidor
client.connect()
  .then(() => {
    console.log('Conectado a Cassandra');
    // Iniciar el servidor Express después de conectar a Cassandra
    app.listen(port, '0.0.0.0', () => {
      console.log(`Servidor corriendo en http://0.0.0.0:${port}`);
    });
  })
  .catch(error => {
    console.error('Error conectando a Cassandra:', error);
  });

app.use(cors());
app.use(bodyParser.json());


async function insertarVenta(venta) {
  const query = `
    INSERT INTO ventas (
      Fecha, Marca, Modelo, Year, Nombre_cliente, Apellido_cliente, 
      Email_cliente, Nombre_empleado, Apellido_empleado, Sucursal, Precio_venta
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;
  const params = [
    venta.Fecha, venta.Marca, venta.Modelo, venta.Year, venta.Nombre_cliente, 
    venta.Apellido_cliente, venta.Email_cliente, venta.Nombre_empleado, 
    venta.Apellido_empleado, venta.Sucursal, venta.Precio_venta
  ];
  try {
    await client.execute(query, params, { prepare: true });
  } catch (error) {
    console.error('Error ejecutando la consulta:', error);
    throw error;
  }
}

async function obtenerClientes() {
  try {
    const query = 'SELECT * FROM clientes';
    const result = await client.execute(query);
    return result.rows;
  } catch (error) {
    console.error('Error ejecutando la consulta:', error);
    return [];
  }
}

async function obtenerAutos() {
  try {
    const query = 'SELECT * FROM autos';
    const result = await client.execute(query);
    return result.rows;
  } catch (error) {
    console.error('Error ejecutando la consulta:', error);
    return [];
  }
}

async function obtenerEmpleadosPorSucursal(nombreSucursal) {
  try {
    if(nombreSucursal == 'CEO'){
      const query = 'SELECT * FROM empleados;';
      const result = await client.execute(query);
      return result.rows;
    }else{
      const query = 'SELECT * FROM empleados WHERE Nombre_sucursal = ? ALLOW FILTERING';
      const params = [nombreSucursal];
      const result = await client.execute(query, params, { prepare: true });
      return result.rows;
    }

  } catch (error) {
    console.error('Error ejecutando la consulta:', error);
    return [];
  }
}

async function obtenerSucursales() {
  try {
    const query = 'SELECT * FROM sucursales';
    const result = await client.execute(query);
    return result.rows;
  } catch (error) {
    console.error('Error ejecutando la consulta:', error);
    return [];
  }
}

async function obtenerDatosPorSucursal(sucursal) {
  try {
    if(sucursal == 'CEO'){
      const query = 'SELECT * FROM ventas;';
      const result = await client.execute(query);
      return result.rows;
    }else{
      const query = 'SELECT * FROM ventas WHERE sucursal = ?  ';
      const result = await client.execute(query, [sucursal], { prepare: true });
      return result.rows;
    }
    
  } catch (error) {
    console.error('Error ejecutando la consulta:', error);
    return [];
  }
}

async function verificarCredenciales(usuario, contrasena) {
  try {
    const query = `SELECT Password, Nombre, Nombre_sucursal, Apellido, Puesto FROM empleados WHERE Usuario = '${usuario}'`;
    const result = await client.execute(query);
    if (result.rowLength === 0) {
      return false; // Usuario no encontrado
    }
    const hashedPassword = result.rows[0].password;
    if (!(hashedPassword === contrasena)) {
      return null; // Contraseña incorrecta
    }
    return {
      nombre: result.rows[0].nombre,
      apellido: result.rows[0].apellido,
      nombre_sucursal: result.rows[0].nombre_sucursal,
      puesto: result.rows[0].puesto,
    };
  } catch (error) {
    console.error('Error ejecutando la consulta:', error);
    return false;
  }
}

async function insertarEmpleado(nombre, apellido, puesto, nombre_sucursal, usuario, password) {
  try {
    const query = 'INSERT INTO empleados (Nombre, Apellido, Puesto, Nombre_sucursal, Usuario, Password) VALUES (?, ?, ?, ?, ?, ?)';
    const params = [nombre, apellido, puesto, nombre_sucursal, usuario, password];
    await client.execute(query, params, { prepare: true });
    return { mensaje: 'Empleado insertado exitosamente' };
  } catch (error) {
    if (error.code === 8705) { // Código de error para clave primaria duplicada
      return { error: 'Usuario ya existe' };
    }
    console.error('Error ejecutando la inserción:', error);
    return { error: 'Error insertando empleado' };
  }
}


// Insertar empleados SingUp 
app.post('/api/singup', async (req, res) => {
  const { nombre, apellido, puesto, nombre_sucursal, usuario, password } = req.body;
  console.log(req.body);
  if (!nombre || !apellido || !puesto || !nombre_sucursal || !usuario || !password) {
    
    return res.status(400).json({ error: 'Faltan datos del empleado' });
  }

  const result = await insertarEmpleado(nombre, apellido, puesto, nombre_sucursal, usuario, password);
  if (result.error) {
    return res.status(400).json(result);
  }
  res.json(result);
});

// Ventas por sucursal 
app.get('/api/ventas', async (req, res) => {
  const sucursal = req.query.sucursal;
  if (!sucursal) {
    return res.status(400).json({ error: 'Falta el parámetro sucursal' });
  }
  try {
    const data = await obtenerDatosPorSucursal(sucursal);
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Error al obtener datos' });
  }
});


// LogIn verifica si existe el usuario (empleado)
app.post('/api/login', async (req, res) => {
  const { usuario, password } = req.body;
  console.log(usuario, " ", password);
  const userData = await verificarCredenciales(usuario, password);
  if (userData) {
    res.json(userData);
  } else {
    res.status(401).json({ error: 'Usuario o contraseña incorrectos' });
  }
});

// Optener las sucursales
app.get('/api/sucursales', async (req, res) => {
  try {
    const data = await obtenerSucursales();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Error al obtener datos' });
  }
});

// Optener los autos
app.get('/api/autos', async (req, res) => {
  try {
    const data = await obtenerAutos();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Error al obtener datos' });
  }
});

app.get('/api/clientes', async (req, res) => {
  try {
    const data = await obtenerClientes();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Error al obtener datos' });
  }
});



// Empleados por sucursal
app.get('/api/empleados', async (req, res) => {
  const nombreSucursal = req.query.sucursal;
  if (!nombreSucursal) {
    return res.status(400).json({ error: 'Falta el nombre de la sucursal' });
  }
  try {
    const data = await obtenerEmpleadosPorSucursal(nombreSucursal);
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Error al obtener datos' });
  }
});

app.post('/api/ventascarros', async (req, res) => {
  const venta = req.body;
  console.log(venta);
  if (!venta) {
    return res.status(400).json({ error: 'Faltan datos de la venta' });
  }
  try {
    await insertarVenta(venta);
    res.status(201).json({ mensaje: 'Venta insertada exitosamente' });
  } catch (error) {
    res.status(500).json({ error: 'Error al insertar la venta' });
  }
});


// Asegúrate de manejar el cierre del cliente correctamente
process.on('SIGINT', () => {
  client.shutdown()
    .then(() => {
      console.log('Cliente de Cassandra cerrado');
      process.exit(0);
    })
    .catch(err => {
      console.error('Error cerrando el cliente de Cassandra:', err);
      process.exit(1);
    });
});
