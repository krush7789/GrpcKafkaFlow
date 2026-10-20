'use client';

import { FormEvent, useRef, useState } from 'react';
import { useRouter } from 'next/navigation';
import { api } from './api';

export default function AuthPage() {
  const router = useRouter();
  const [mode, setMode] = useState<'signup' | 'login'>('signup');
  const [form, setForm] = useState({ name: '', email: '', password: '' });
  const [error, setError] = useState('');
  // One key per signup ATTEMPT: clicking "Sign up" again after a timeout reuses it,
  // so auth returns the original account instead of "email already registered".
  const idempotencyKey = useRef(crypto.randomUUID());

  async function submit(e: FormEvent) {
    e.preventDefault();
    setError('');
    try {
      // Browser -> gateway (REST). Gateway -> auth-service (gRPC).
      const { token } = await api<{ userId: string; token: string }>(`/auth/${mode}`, {
        method: 'POST',
        body: JSON.stringify(form),
        headers: mode === 'signup' ? { 'Idempotency-Key': idempotencyKey.current } : {},
      });
      localStorage.setItem('token', token);
      router.push('/dashboard');
    } catch (err) {
      setError((err as Error).message);
    }
  }

  const field = (key: keyof typeof form) => ({
    value: form[key],
    onChange: (e: React.ChangeEvent<HTMLInputElement>) => {
      idempotencyKey.current = crypto.randomUUID(); // different data = a different request
      setForm({ ...form, [key]: e.target.value });
    },
  });

  return (
    <div className="card">
      <h1>{mode === 'signup' ? 'Create account' : 'Log in'}</h1>
      <form onSubmit={submit}>
        {mode === 'signup' && (
          <label>Name<input required {...field('name')} /></label>
        )}
        <label>Email<input type="email" required {...field('email')} /></label>
        <label>Password<input type="password" required minLength={6} {...field('password')} /></label>
        {error && <p className="error">{error}</p>}
        <button type="submit">{mode === 'signup' ? 'Sign up' : 'Log in'}</button>
      </form>
      <p className="muted">
        {mode === 'signup' ? 'Have an account? ' : 'New here? '}
        <button className="link" onClick={() => setMode(mode === 'signup' ? 'login' : 'signup')}>
          {mode === 'signup' ? 'Log in' : 'Sign up'}
        </button>
      </p>
    </div>
  );
}
