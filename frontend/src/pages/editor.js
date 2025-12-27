import { useState } from "react";
import dynamic from "next/dynamic";
import * as XLSX from "xlsx";

// Dynamically import Monaco Editor
const MonacoEditor = dynamic(() => import("@monaco-editor/react"), { ssr: false });

export default function EditorPage() {
  const [code, setCode] = useState("");
  const [output, setOutput] = useState(null);
  const [log, setLog] = useState([]);
  const [preview, setPreview] = useState([]);
  const [fileName, setFileName] = useState("");
  const [workbook, setWorkbook] = useState(null);
  const [sheetNames, setSheetNames] = useState([]);
  const [selectedSheet, setSelectedSheet] = useState("");

  const runScript = async () => {
    try {
      const res = await fetch("http://localhost:8000/run-script", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ code, output_format: "json" }),
      });
      const data = await res.json();
      setOutput(data);
      setLog((prev) => [...prev, "Script executed successfully"]);
    } catch (err) {
      setLog((prev) => [...prev, `Error: ${err.message}`]);
    }
  };

  const clearScript = () => {
    setCode("");
    setOutput(null);
    setLog((prev) => [...prev, "Editor cleared"]);
  };

  const handleFileUpload = (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setFileName(file.name);

    const reader = new FileReader();
    reader.onload = (event) => {
      const data = new Uint8Array(event.target.result);

      if (file.name.endsWith(".csv")) {
        // Handle CSV
        const text = new TextDecoder().decode(data);
        const rows = text.split("\n").slice(0, 10);
        setPreview(rows.map((row) => row.split(",")));
        setLog((prev) => [...prev, `Loaded CSV: ${file.name}`]);
      } else {
        // Handle Excel
        const wb = XLSX.read(data, { type: "array" });
        setWorkbook(wb);
        setSheetNames(wb.SheetNames);
        setSelectedSheet(wb.SheetNames[0]); // default to first sheet
        const sheet = wb.Sheets[wb.SheetNames[0]];
        const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 }).slice(0, 10);
        setPreview(rows);
        setLog((prev) => [...prev, `Loaded Excel: ${file.name}`]);
      }
    };

    reader.readAsArrayBuffer(file);
  };

  const handleSheetChange = (e) => {
    const sheetName = e.target.value;
    setSelectedSheet(sheetName);
    if (workbook) {
      const sheet = workbook.Sheets[sheetName];
      const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 }).slice(0, 10);
      setPreview(rows);
      setLog((prev) => [...prev, `Switched to sheet: ${sheetName}`]);
    }
  };

  const insertSetStatement = () => {
    if (!fileName) {
      setLog((prev) => [...prev, "No file selected"]);
      return;
    }
    const sheetInfo = selectedSheet ? ` (sheet=${selectedSheet})` : "";
    setCode((prev) => `SET path="${fileName}"${sheetInfo};\n` + prev);
    setLog((prev) => [...prev, `Inserted SET statement for ${fileName}${sheetInfo}`]);
  };

  return (
    <div style={{ display: "flex", height: "100vh" }}>
      {/* Dataset browser */}
      <div style={{ flex: 1, borderRight: "1px solid #ccc", padding: "10px" }}>
        <h2>Dataset Browser</h2>
        <input type="file" accept=".csv,.xlsx,.xls" onChange={handleFileUpload} />
        {sheetNames.length > 0 && (
          <div style={{ marginTop: "10px" }}>
            <label>Select sheet/ Tab: </label>
            <select value={selectedSheet} onChange={handleSheetChange}>
              {sheetNames.map((name) => (
                <option key={name} value={name}>{name}</option>
              ))}
            </select>
          </div>
        )}
        <button onClick={insertSetStatement} style={{ marginTop: "10px" }}>Insert SET</button>
        <div style={{ marginTop: "10px", overflow: "auto", height: "60vh", background: "#f9f9f9", padding: "5px" }}>
          <table>
            <tbody>
              {preview.map((row, i) => (
                <tr key={i}>
                  {row.map((cell, j) => (
                    <td key={j} style={{ padding: "4px", border: "1px solid #ddd" }}>{cell}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Editor panel */}
      <div style={{ flex: 2, borderRight: "1px solid #ccc", padding: "10px" }}>
        <h2>Script Editor</h2>
        <MonacoEditor
          height="70vh"
          defaultLanguage="plaintext"
          value={code}
          onChange={(val) => setCode(val || "")}
        />
        <div style={{ marginTop: "10px" }}>
          <button onClick={runScript}>Run</button>
          <button onClick={clearScript} style={{ marginLeft: "10px" }}>Clear</button>
        </div>
      </div>

      {/* Output viewer */}
      <div style={{ flex: 2, borderRight: "1px solid #ccc", padding: "10px" }}>
        <h2>Output Viewer</h2>
        <pre style={{ background: "#f9f9f9", padding: "10px", height: "70vh", overflow: "auto" }}>
          {output ? JSON.stringify(output, null, 2) : "No output yet"}
        </pre>
      </div>

      {/* Log window */}
      <div style={{ flex: 1, padding: "10px" }}>
        <h2>Log Window</h2>
        <div style={{ background: "#f0f0f0", padding: "10px", height: "70vh", overflow: "auto" }}>
          {log.map((entry, i) => (
            <div key={i}>{entry}</div>
          ))}
        </div>
      </div>
    </div>
  );
}
