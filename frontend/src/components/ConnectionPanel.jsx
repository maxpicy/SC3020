import { useState, useEffect } from 'react';
import { getConnectionStatus, updateConnection } from '../api';

const DEFAULT_FORM = {
  host: 'localhost',
  port: 5432,
  dbname: '',
  user: '',
  password: '',
};

export default function ConnectionPanel({ connected, setConnected, onConnected }) {
  const [form, setForm] = useState(DEFAULT_FORM);
  const [collapsed, setCollapsed] = useState(false);
  const [statusMsg, setStatusMsg] = useState('Checking connection...');
  const [error, setError] = useState(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    getConnectionStatus()
      .then(s => {
        if (s.config) {
          setForm(f => ({
            ...f,
            host: s.config.host ?? f.host,
            port: s.config.port ?? f.port,
            dbname: s.config.dbname ?? f.dbname,
            user: s.config.user ?? f.user,
          }));
        }
        if (s.connected) {
          setConnected(true);
          setCollapsed(true);
          setStatusMsg('');
          if (onConnected) onConnected(s.config);
        } else {
          setConnected(false);
          setStatusMsg(s.error ? `Not connected: ${s.error}` : 'Not connected');
        }
      })
      .catch(e => {
        setConnected(false);
        setStatusMsg(`Error: ${e.message}`);
      });
  }, [setConnected]);

  const handleChange = (field) => (e) => {
    const val = field === 'port' ? e.target.value.replace(/[^0-9]/g, '') : e.target.value;
    setForm(f => ({ ...f, [field]: val }));
  };

  const handleConnect = async (e) => {
    e?.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const payload = {
        ...form,
        port: parseInt(form.port, 10) || 5432,
      };
      const res = await updateConnection(payload);
      setConnected(true);
      setCollapsed(true);
      setStatusMsg('');
      if (onConnected) onConnected(res.config || payload);
    } catch (err) {
      setConnected(false);
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  if (collapsed && connected) {
    return (
      <div className="connection-panel connection-panel-collapsed">
        <div className="connection-summary">
          <span className="connection-dot connected" />
          <span className="connection-info">
            Connected to <strong>{form.dbname}</strong> as <strong>{form.user}</strong> @ {form.host}:{form.port}
          </span>
          <button className="connection-edit-btn" onClick={() => setCollapsed(false)}>
            Change
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="connection-panel">
      <div className="connection-header">
        <h2>Database Connection</h2>
        <span className={`connection-dot ${connected ? 'connected' : 'disconnected'}`} />
      </div>

      <form className="connection-form" onSubmit={handleConnect}>
        <div className="connection-row">
          <label>
            Host
            <input
              type="text"
              value={form.host}
              onChange={handleChange('host')}
              placeholder="localhost"
              required
            />
          </label>
          <label className="connection-port">
            Port
            <input
              type="text"
              value={form.port}
              onChange={handleChange('port')}
              placeholder="5432"
              required
            />
          </label>
        </div>

        <label>
          Database
          <input
            type="text"
            value={form.dbname}
            onChange={handleChange('dbname')}
            placeholder="TPC-H"
            required
          />
        </label>

        <div className="connection-row">
          <label>
            User
            <input
              type="text"
              value={form.user}
              onChange={handleChange('user')}
              placeholder="postgres"
              required
            />
          </label>
          <label>
            Password
            <input
              type="password"
              value={form.password}
              onChange={handleChange('password')}
              placeholder="(leave empty if none)"
            />
          </label>
        </div>

        <div className="connection-actions">
          <button type="submit" disabled={busy}>
            {busy ? 'Connecting...' : 'Connect'}
          </button>
          {connected && (
            <button
              type="button"
              className="connection-cancel-btn"
              onClick={() => setCollapsed(true)}
            >
              Cancel
            </button>
          )}
          {statusMsg && !error && <span className="connection-status">{statusMsg}</span>}
        </div>

        {error && <div className="connection-error">{error}</div>}
      </form>
    </div>
  );
}
