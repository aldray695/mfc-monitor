
const mysql = require("mysql2/promise");
const crypto = require("crypto");

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
    let data = req.body;

    // Jika payload berupa string JSON
    if (typeof data === "string") {
      data = JSON.parse(data);
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

    // ==========================================
    // FORMAT MQTT: 4 CHANNEL SEKALIGUS
    // ==========================================
    if (Array.isArray(data.channels)) {
      const timestamp = data.timestamp;

      let measuredAt;

      if (timestamp !== undefined && timestamp !== null) {
        const ts = Number(timestamp);

        // Unix timestamp seconds atau milliseconds
        const date = new Date(
          ts < 1000000000000 ? ts * 1000 : ts
        );

        if (isNaN(date.getTime())) {
          return res.status(400).json({
            success: false,
            message: "Invalid timestamp",
          });
        }

        measuredAt = date.toISOString()
          .slice(0, 19)
          .replace("T", " ");
      } else {
        measuredAt = new Date()
          .toISOString()
          .slice(0, 19)
          .replace("T", " ");
      }

      const results = [];

      for (const channelData of data.channels) {
        const channel = Number(channelData.channel);
        const voltage = Number(channelData.voltage);

        // Validasi channel dan voltage
        if (
          !Number.isInteger(channel) ||
          channel < 1 ||
          channel > 4 ||
          !Number.isFinite(voltage)
        ) {
          results.push({
            channel: channelData.channel,
            success: false,
            message: "Invalid channel or voltage",
          });

          continue;
        }

        // Mapping channel ke reactor
        const reactorCode = `MFC-0${channel}`;

        const [reactors] = await connection.execute(
          "SELECT reactor_id FROM reactors WHERE reactor_code = ?",
          [reactorCode]
        );

        if (reactors.length === 0) {
          results.push({
            channel,
            reactor_code: reactorCode,
            success: false,
            message: "Reactor not found",
          });

          continue;
        }

        const reactorId = reactors[0].reactor_id;

        // ID unik berdasarkan waktu, channel, dan voltage
        const uniqueText = `${timestamp || measuredAt}-${channel}-${voltage}`;

        const messageId = crypto
          .createHash("sha256")
          .update(uniqueText)
          .digest("hex")
          .slice(0, 32);

        // Simpan pengukuran ke TiDB
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
          VALUES (?, ?, ?, ?, NULL, NULL, NULL, 'valid')`,
          [
            messageId,
            reactorId,
            measuredAt,
            voltage,
          ]
        );

        results.push({
          channel,
          reactor_code: reactorCode,
          voltage_v: voltage,
          saved: result.affectedRows === 1,
        });
      }

      return res.status(200).json({
        success: true,
        message: "MQTT channels processed",
        results,
      });
    }

    // ==========================================
    // FORMAT API LAMA: SINGLE REACTOR
    // ==========================================

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

    // Cari reactor
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

    // Konversi waktu
    const measuredAt = new Date(data.measured_at)
      .toISOString()
      .slice(0, 19)
      .replace("T", " ");

    // Simpan data lama
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
