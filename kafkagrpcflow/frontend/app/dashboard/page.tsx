'use client';

import { useCallback, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { api, ApiError } from '../api';

type Profile = { userId: string; name: string; email: string; bio: string; followersCount: number; followingCount: number };

export default function Dashboard() {
  const router = useRouter();
  const [me, setMe] = useState<Profile | null>(null);
  const [people, setPeople] = useState<Profile[]>([]);
  const [following, setFollowing] = useState<string[]>([]);
  const [bio, setBio] = useState('');
  const [status, setStatus] = useState('Loading…');

  const load = useCallback(async () => {
    try {
      const profile = await api<Profile>('/profiles/me');
      setMe(profile);
      setBio(profile.bio);
      setPeople((await api<Profile[]>('/profiles')).filter((p) => p.userId !== profile.userId));
      setFollowing((await api<{ followeeIds: string[] }>('/interactions/following')).followeeIds);
      setStatus('');
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) return router.push('/');
      if (err instanceof ApiError && err.status === 404) {
        // Signup answered instantly, but profile-service creates the profile only when it
        // consumes the "user.created" event from Kafka. That's eventual consistency, visible.
        setStatus('Your profile is being created (waiting for the user.created Kafka event)…');
        setTimeout(load, 1000);
        return;
      }
      setStatus((err as Error).message);
    }
  }, [router]);

  useEffect(() => {
    load();
  }, [load]);

  async function saveBio() {
    setMe(await api<Profile>('/profiles/me', { method: 'PATCH', body: JSON.stringify({ bio }) }));
  }

  async function follow(userId: string) {
    const res = await api<{ message: string }>(`/interactions/follow/${userId}`, { method: 'POST' });
    setFollowing([...following, userId]);
    // The follow is saved now; counters update when profile-service consumes "user.followed".
    setStatus(`${res.message}. Counters update once profile-service consumes user.followed…`);
    setTimeout(load, 1500);
  }

  function logout() {
    localStorage.removeItem('token');
    router.push('/');
  }

  return (
    <>
      {status && <p className="status">{status}</p>}
      {me && (
        <div className="card">
          <div className="row" style={{ borderTop: 0 }}>
            <h1 style={{ margin: 0 }}>Hi, {me.name}</h1>
            <button className="link" onClick={logout}>Log out</button>
          </div>
          <p className="muted">{me.email} · {me.followersCount} followers · {me.followingCount} following</p>
          <label>Bio<textarea rows={2} maxLength={280} value={bio} onChange={(e) => setBio(e.target.value)} /></label>
          <button onClick={saveBio}>Save bio</button>
        </div>
      )}
      {me && (
        <div className="card">
          <h2>People</h2>
          {people.length === 0 && <p className="muted">No one else yet. Sign up a second user in another browser.</p>}
          {people.map((p) => (
            <div className="row" key={p.userId}>
              <div>
                <strong>{p.name}</strong>
                <div className="muted">{p.bio || 'No bio'} · {p.followersCount} followers</div>
              </div>
              <button disabled={following.includes(p.userId)} onClick={() => follow(p.userId)}>
                {following.includes(p.userId) ? 'Following' : 'Follow'}
              </button>
            </div>
          ))}
        </div>
      )}
    </>
  );
}
