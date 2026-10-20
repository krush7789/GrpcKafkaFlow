import { ClientProviderOptions, ClientsModule, Transport } from '@nestjs/microservices';
import { join } from 'path';
import { Observable } from 'rxjs';

export const AUTH = 'AUTH_PACKAGE';
export const PROFILE = 'PROFILE_PACKAGE';
export const INTERACTION = 'INTERACTION_PACKAGE';

const grpcClient = (name: string, pkg: string, file: string, url: string): ClientProviderOptions => ({
  name,
  transport: Transport.GRPC,
  options: {
    package: pkg,
    protoPath: join(__dirname, '../../proto', file),
    url,
    // defaults: true -> fields at their default value (0, false, '') still show up in the JS object
    loader: { defaults: true },
  },
});

// One long-lived HTTP/2 connection per service, reused by every request.
export const grpcClients = ClientsModule.register([
  grpcClient(AUTH, 'auth', 'auth.proto', process.env.AUTH_GRPC_URL ?? 'localhost:50051'),
  grpcClient(PROFILE, 'profile', 'profile.proto', process.env.PROFILE_GRPC_URL ?? 'localhost:50052'),
  grpcClient(INTERACTION, 'interaction', 'interaction.proto', process.env.INTERACTION_GRPC_URL ?? 'localhost:50053'),
]);

// TypeScript mirrors of the .proto files. (Bigger projects generate these with ts-proto.)
// Method names are the proto rpc names in camelCase: rpc SignUp -> signUp().
export interface AuthResponse { userId: string; token: string }
export interface AuthService {
  signUp(req: { name: string; email: string; password: string; idempotencyKey: string }): Observable<AuthResponse>;
  login(req: { email: string; password: string }): Observable<AuthResponse>;
  validateToken(req: { token: string }): Observable<{ valid: boolean; userId: string; email: string }>;
}

export interface Profile {
  userId: string; name: string; email: string; bio: string; followersCount: number; followingCount: number;
}
export interface ProfileService {
  getProfile(req: { userId: string }): Observable<Profile>;
  updateProfile(req: { userId: string; bio: string }): Observable<Profile>;
  listProfiles(req: {}): Observable<{ profiles: Profile[] }>;
}

export interface InteractionService {
  follow(req: { followerId: string; followeeId: string }): Observable<{ success: boolean; message: string }>;
  getFollowing(req: { userId: string }): Observable<{ followeeIds: string[] }>;
}
