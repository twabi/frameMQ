import random


class Particle:
    def __init__(self, position=None, velocity=None, p_best_position=None, g_best_position=None):
        self.position = {
            'partitions': position['partitions'],
            'chunk_number': position['chunk_number'],
            'quality': position['quality'],
            'level': position['level']
        }
        self.velocity = {
            'partitions': velocity['partitions'],
            'chunk_number': velocity['chunk_number'],
            'quality': velocity['quality'],
            'level': velocity['level']
        }

        self.p_best = float('inf')  # Start with the lowest fitness
        self.p_best_position = self.position.copy()  # Store the position corresponding to p_best

        # Global best (g_best) will be handled by the frameMQ PSO class
        self.g_best_position = g_best_position.copy() if g_best_position else None

    def update_velocity(self, w, c1, c2):
        r1 = random.random()
        r2 = random.random()

        cognitive = [
            r1 * (self.p_best_position['partitions'] - self.position['partitions']),
            r1 * (self.p_best_position['chunk_number'] - self.position['chunk_number']),
            r1 * (self.p_best_position['quality'] - self.position['quality']),
            r1 * (self.p_best_position['level'] - self.position['level'])
        ]

        if self.g_best_position:
            social = [
                r2 * (self.g_best_position['partitions'] - self.position['partitions']),
                r2 * (self.g_best_position['chunk_number'] - self.position['chunk_number']),
                r2 * (self.g_best_position['quality'] - self.position['quality']),
                r2 * (self.g_best_position['level'] - self.position['level'])
            ]

            self.velocity['partitions'] = w * self.velocity['partitions'] + c1 * cognitive[0] + c2 * social[0]
            self.velocity['chunk_number'] = w * self.velocity['chunk_number'] + c1 * cognitive[1] + c2 * social[1]
            self.velocity['quality'] = w * self.velocity['quality'] + c1 * cognitive[2] + c2 * social[2]
            self.velocity['level'] = w * self.velocity['level'] + c1 * cognitive[3] + c2 * social[3]

    def update_position(self):
        new_parts = self.position['partitions'] + self.velocity['partitions']
        new_chunks = self.position['chunk_number'] + self.velocity['chunk_number']
        new_qual = self.position['quality'] + self.velocity['quality']
        new_level = self.position['level'] + self.velocity['level']

        # Clamp the values within their respective bounds
        self.position['partitions'] = max(2, min(400, new_parts))
        self.position['chunk_number'] = max(10, min(500, new_chunks))
        self.position['quality'] = max(15, min(90, new_qual))
        self.position['level'] = max(0, min(2, new_level))
