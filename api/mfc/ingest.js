const mysql = require("mysql2/promise");

export default async function handler(req, res) {
  // Hanya menerima POST
  if (req.method !== "POST") {
    return res.status(405).json({
      success: false,
      message: "Method not allowed",
    });
  }

  // Autentikasi webhook
  const token = req.headers.authorization;
  if (token !== `Bearer ${process.env.WEBHOOK_SECRET}`) {
    return res.status(401).json({
      success: false,
      message: "Unauthorized",
    });
  }

  let connection;

  try {
    const data = req.body;

    // Validasi data
    const requiredFields = [
      "message_id",
      "reactor_code",
      "measured_at",
      "voltage_v",
      "current_ma",
      "resistance_ohm",
      "power_mw",
    ];

    for (const field of requiredFields) {
      if (data[field] === undefined || data[field] === null) {
        return res.status(400).json({
          success: false,
          message: `Missing field: ${field}`,
        });
      }
    }

    // Koneksi ke TiDB
    connection = await mysql.createConnection({
      host: process.env.DB_HOST,
      port: Number(process.env.DB_PORT || 4000),
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      ssl: {
        rejectUnauthorized: true,
      },
    });

    // Cari reactor_id
    const [reactors] = await connection.execute(
      "SELECT reactor_id FROM reactors WHERE reactor_code = ?",
      [data.reactor_code]
    );

    if (reactors.length === 0) {
      return res.status(404).json({
        success: false,
        message: "Reactor not found",
      });
    }

    const reactorId = reactors[0].reactor_id;

    // Konversi waktu ke DATETIME MySQL
    const measuredAt = new Date(data.measured_at)
      .toISOString()
      .slice(0, 19)
      .replace("T", " ");

    // Simpan data pengukuran
    const [result] = await connection.execute(
      `INSERT IGNORE INTO measurements
      (
        message_id,
        reactor_id,
        measured_at,
        voltage_v,
        current_ma,
        resistance_ohm,
        power_mw,
        data_status
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, 'valid')`,
      [
        data.message_id,
        reactorId,
        measuredAt,
        data.voltage_v,
        data.current_ma,
        data.resistance_ohm,
        data.power_mw,
      ]
    );

    return res.status(200).json({
      success: true,
      message: result.affectedRows === 1
        ? "Data successfully saved to TiDB"
        : "Duplicate data ignored",
      reactor_code: data.reactor_code,
      message_id: data.message_id,
    });

  } catch (error) {
    console.error("TiDB error:", error.message);

    return res.status(500).json({
      success: false,
      message: "Failed to save data",
    });

  } finally {
    if (connection) {
      await connection.end();
    }
  }
}
