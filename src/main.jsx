import React, { StrictMode, useState, useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import SAFManager from '../SAF_Certificate_Manager.jsx'
import { supabase } from './supabase.js'

class AppErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error) {
    return { error };
  }

  componentDidCatch(error) {
    console.error('App render error:', error);
  }

  render() {
    if (!this.state.error) return this.props.children;

    return (
      <div
        style={{
          minHeight: '100vh',
          background: '#030d1a',
          color: '#c8dff0',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          padding: 24,
          fontFamily: "'Space Mono', monospace",
        }}
      >
        <div
          style={{
            width: 'min(860px, 92vw)',
            background: '#0a1628',
            border: '1px solid #6b2020',
            borderRadius: 10,
            padding: 24,
          }}
        >
          <div style={{ color: '#ff6b6b', fontSize: 12, letterSpacing: 2, marginBottom: 12 }}>RUNTIME ERROR</div>
          <div style={{ color: '#e0f0ff', fontSize: 14, marginBottom: 12 }}>
            The app crashed while rendering. The error is shown below instead of a blank page.
          </div>
          <pre
            style={{
              margin: 0,
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              color: '#ffb3b3',
              fontSize: 12,
              lineHeight: 1.5,
            }}
          >
            {String(this.state.error?.stack || this.state.error?.message || this.state.error)}
          </pre>
        </div>
      </div>
    );
  }
}

function LoginPage({ onLogin }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    const { error: authErr } = await supabase.auth.signInWithPassword({ email, password });
    if (authErr) setError(authErr.message);
    setLoading(false);
  };

  return (
    <div style={{
      minHeight: '100vh', background: '#030d1a',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      fontFamily: "'Space Mono', monospace"
    }}>
      <div style={{
        width: 360, background: '#0a1628',
        border: '1px solid #0d3060', borderRadius: 10,
        padding: '40px 36px'
      }}>
        <div style={{ marginBottom: 32, textAlign: 'center' }}>
          <div style={{ fontSize: 11, letterSpacing: 3, color: '#4a9fd4', marginBottom: 6 }}>
            TITAN AVIATION FUELS
          </div>
          <div style={{ fontSize: 18, color: '#c8dff0', letterSpacing: 1 }}>
            SAF Certificate Manager
          </div>
        </div>

        <form onSubmit={handleSubmit}>
          <div style={{ marginBottom: 16 }}>
            <label style={{ display: 'block', fontSize: 10, letterSpacing: 2, color: '#4a9fd4', marginBottom: 6 }}>
              EMAIL
            </label>
            <input
              type="email"
              value={email}
              onChange={e => setEmail(e.target.value)}
              required
              autoFocus
              style={{
                width: '100%', boxSizing: 'border-box',
                background: '#030d1a', border: '1px solid #0d3060',
                borderRadius: 5, padding: '9px 12px',
                color: '#c8dff0', fontSize: 13,
                fontFamily: "'Space Mono', monospace",
                outline: 'none'
              }}
            />
          </div>

          <div style={{ marginBottom: 24 }}>
            <label style={{ display: 'block', fontSize: 10, letterSpacing: 2, color: '#4a9fd4', marginBottom: 6 }}>
              PASSWORD
            </label>
            <input
              type="password"
              value={password}
              onChange={e => setPassword(e.target.value)}
              required
              style={{
                width: '100%', boxSizing: 'border-box',
                background: '#030d1a', border: '1px solid #0d3060',
                borderRadius: 5, padding: '9px 12px',
                color: '#c8dff0', fontSize: 13,
                fontFamily: "'Space Mono', monospace",
                outline: 'none'
              }}
            />
          </div>

          {error && (
            <div style={{
              marginBottom: 16, padding: '8px 12px',
              background: '#1a0a0a', border: '1px solid #6b2020',
              borderRadius: 5, color: '#ff6b6b', fontSize: 11
            }}>
              {error}
            </div>
          )}

          <button
            type="submit"
            disabled={loading}
            style={{
              width: '100%', padding: '10px',
              background: loading ? '#0d2040' : 'linear-gradient(135deg,#0050aa,#00bfff)',
              color: '#fff', border: 'none', borderRadius: 5,
              fontFamily: "'Space Mono', monospace",
              fontSize: 11, letterSpacing: 2, cursor: loading ? 'default' : 'pointer'
            }}
          >
            {loading ? 'SIGNING IN…' : 'SIGN IN'}
          </button>
        </form>
      </div>
    </div>
  );
}

function App() {
  const [session, setSession] = useState(undefined); // undefined = loading

  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session } }) => setSession(session));
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, session) => {
      setSession(session);
    });
    return () => subscription.unsubscribe();
  }, []);

  if (session === undefined) {
    // Still checking session — show blank dark screen to avoid flash
    return <div style={{ minHeight: '100vh', background: '#030d1a' }} />;
  }

  if (!session) return <LoginPage />;

  return <SAFManager onLogout={() => supabase.auth.signOut()} userEmail={session.user.email} />;
}

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <AppErrorBoundary>
      <App />
    </AppErrorBoundary>
  </StrictMode>
)
