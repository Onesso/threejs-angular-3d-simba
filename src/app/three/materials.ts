import * as THREE from 'three';

function createBrickTexture(): THREE.CanvasTexture {
  const canvas = document.createElement('canvas');
  canvas.width = 512;
  canvas.height = 512;
  const ctx = canvas.getContext('2d')!;

  const brickW = 64; // brick width in pixels
  const brickH = 24; // brick height
  const mortarSize = 3; // mortar joint thickness
  const rows = Math.ceil(canvas.height / (brickH + mortarSize));
  const cols = Math.ceil(canvas.width / (brickW + mortarSize)) + 1;

  // Fill with mortar color
  ctx.fillStyle = '#8a8580';
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  // Brick color palette (warm reddish-brown variation like the screenshot)
  const brickColors = [
    '#a0522d', '#8b4513', '#b5651d', '#9e5c3a',
    '#c4703f', '#7a4422', '#a86032', '#bf7040',
    '#8d5533', '#b06830', '#6e3b1e', '#c97d4a',
    '#94583a', '#ab6940', '#7f4828', '#be7545',
  ];

  for (let row = 0; row < rows; row++) {
    const y = row * (brickH + mortarSize);
    const offset = row % 2 === 0 ? 0 : -(brickW / 2 + mortarSize / 2); // running bond

    for (let col = -1; col < cols; col++) {
      const x = col * (brickW + mortarSize) + offset;

      // Pick a random brick color
      const baseColor = brickColors[Math.floor(Math.random() * brickColors.length)];
      ctx.fillStyle = baseColor;
      ctx.fillRect(x, y, brickW, brickH);

      // Add subtle variation/noise within each brick
      for (let i = 0; i < 15; i++) {
        const nx = x + Math.random() * brickW;
        const ny = y + Math.random() * brickH;
        const shade = Math.random() > 0.5 ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.08)';
        ctx.fillStyle = shade;
        ctx.fillRect(nx, ny, Math.random() * 12 + 2, Math.random() * 4 + 1);
      }
    }
  }

  const texture = new THREE.CanvasTexture(canvas);
  texture.wrapS = THREE.RepeatWrapping;
  texture.wrapT = THREE.RepeatWrapping;
  return texture;
}

function createBrickBumpMap(): THREE.CanvasTexture {
  const canvas = document.createElement('canvas');
  canvas.width = 512;
  canvas.height = 512;
  const ctx = canvas.getContext('2d')!;

  const brickW = 64;
  const brickH = 24;
  const mortarSize = 3;
  const rows = Math.ceil(canvas.height / (brickH + mortarSize));
  const cols = Math.ceil(canvas.width / (brickW + mortarSize)) + 1;

  // Mortar is dark (recessed)
  ctx.fillStyle = '#000000';
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  // Bricks are light (raised)
  for (let row = 0; row < rows; row++) {
    const y = row * (brickH + mortarSize);
    const offset = row % 2 === 0 ? 0 : -(brickW / 2 + mortarSize / 2);

    for (let col = -1; col < cols; col++) {
      const x = col * (brickW + mortarSize) + offset;
      // Slight variation in height per brick
      const b = 180 + Math.floor(Math.random() * 40);
      ctx.fillStyle = `rgb(${b},${b},${b})`;
      ctx.fillRect(x, y, brickW, brickH);
    }
  }

  const texture = new THREE.CanvasTexture(canvas);
  texture.wrapS = THREE.RepeatWrapping;
  texture.wrapT = THREE.RepeatWrapping;
  return texture;
}

function createGrassTexture(): THREE.CanvasTexture {
  const canvas = document.createElement('canvas');
  canvas.width = 256;
  canvas.height = 256;
  const ctx = canvas.getContext('2d')!;

  ctx.fillStyle = '#4a8c3f';
  ctx.fillRect(0, 0, 256, 256);

  // Add noise for grass variation
  for (let i = 0; i < 5000; i++) {
    const x = Math.random() * 256;
    const y = Math.random() * 256;
    const g = Math.floor(100 + Math.random() * 80);
    ctx.fillStyle = `rgb(${Math.floor(g * 0.5)},${g},${Math.floor(g * 0.3)})`;
    ctx.fillRect(x, y, 2, 2);
  }

  const texture = new THREE.CanvasTexture(canvas);
  texture.wrapS = THREE.RepeatWrapping;
  texture.wrapT = THREE.RepeatWrapping;
  texture.repeat.set(20, 20);
  return texture;
}

function createWoodFloorTexture(): THREE.CanvasTexture {
  const canvas = document.createElement('canvas');
  canvas.width = 256;
  canvas.height = 256;
  const ctx = canvas.getContext('2d')!;

  ctx.fillStyle = '#b8956a';
  ctx.fillRect(0, 0, 256, 256);

  // Wood grain lines
  for (let y = 0; y < 256; y += 2) {
    const brightness = 160 + Math.sin(y * 0.3) * 20 + Math.random() * 15;
    ctx.fillStyle = `rgb(${Math.floor(brightness)},${Math.floor(brightness * 0.7)},${Math.floor(brightness * 0.4)})`;
    ctx.fillRect(0, y, 256, 1);
  }

  // Plank divisions
  for (let x = 0; x < 256; x += 32) {
    ctx.fillStyle = 'rgba(0,0,0,0.1)';
    ctx.fillRect(x, 0, 1, 256);
  }

  const texture = new THREE.CanvasTexture(canvas);
  texture.wrapS = THREE.RepeatWrapping;
  texture.wrapT = THREE.RepeatWrapping;
  texture.repeat.set(4, 4);
  return texture;
}

export function createMaterials() {
  const brickMap = createBrickTexture();
  const brickBump = createBrickBumpMap();

  return {
    brick: (repeatX = 3, repeatY = 2) => {
      const map = brickMap.clone();
      map.repeat.set(repeatX, repeatY);
      map.needsUpdate = true;
      const bump = brickBump.clone();
      bump.repeat.set(repeatX, repeatY);
      bump.needsUpdate = true;
      return new THREE.MeshStandardMaterial({
        map,
        bumpMap: bump,
        bumpScale: 0.04,
        metalness: 0.0,
        roughness: 0.85,
        side: THREE.DoubleSide,
      });
    },

    glass: new THREE.MeshPhysicalMaterial({
      color: 0x88ccff,
      metalness: 0.1,
      roughness: 0.05,
      transparent: true,
      opacity: 0.25,
      side: THREE.DoubleSide,
    }),

    steelFrame: new THREE.MeshStandardMaterial({
      color: 0x1a1a1a,
      metalness: 0.8,
      roughness: 0.3,
    }),

    steelColumn: new THREE.MeshStandardMaterial({
      color: 0x444444,
      metalness: 0.7,
      roughness: 0.35,
    }),

    concrete: new THREE.MeshStandardMaterial({
      color: 0x999999,
      metalness: 0.0,
      roughness: 0.9,
    }),

    grass: new THREE.MeshStandardMaterial({
      map: createGrassTexture(),
      metalness: 0.0,
      roughness: 0.95,
    }),

    roofPanel: new THREE.MeshStandardMaterial({
      color: 0x6b2020,
      metalness: 0.4,
      roughness: 0.5,
      side: THREE.DoubleSide,
    }),

    roofTop: new THREE.MeshStandardMaterial({
      color: 0x2a2a2a,
      metalness: 0.1,
      roughness: 0.8,
      side: THREE.DoubleSide,
    }),

    roofWood: new THREE.MeshStandardMaterial({
      color: 0x8b5e3c,
      metalness: 0.0,
      roughness: 0.75,
      side: THREE.DoubleSide,
    }),

    roofFascia: new THREE.MeshStandardMaterial({
      color: 0x3a3a3a,
      metalness: 0.2,
      roughness: 0.6,
    }),

    interiorWall: new THREE.MeshStandardMaterial({
      color: 0xf5f0e8,
      metalness: 0.0,
      roughness: 0.9,
      side: THREE.DoubleSide,
    }),

    interiorFloor: new THREE.MeshStandardMaterial({
      map: createWoodFloorTexture(),
      metalness: 0.1,
      roughness: 0.7,
    }),

    deckTile: (() => {
      const canvas = document.createElement('canvas');
      canvas.width = 512;
      canvas.height = 512;
      const ctx = canvas.getContext('2d')!;

      const tileW = 128;
      const tileH = 512;
      const grout = 4;
      const cols = Math.ceil(canvas.width / (tileW + grout));

      // Grout background
      ctx.fillStyle = '#6b6158';
      ctx.fillRect(0, 0, 512, 512);

      // Wood-look tiles
      for (let col = 0; col < cols; col++) {
        const x = col * (tileW + grout);
        // Base brown tone with variation per tile
        const r = 140 + Math.floor(Math.random() * 30);
        const g = 90 + Math.floor(Math.random() * 25);
        const b = 55 + Math.floor(Math.random() * 20);
        ctx.fillStyle = `rgb(${r},${g},${b})`;
        ctx.fillRect(x, 0, tileW, tileH);

        // Wood grain lines
        for (let y = 0; y < tileH; y += 3) {
          const shade = Math.random() > 0.5 ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.06)';
          ctx.fillStyle = shade;
          ctx.fillRect(x + 2, y, tileW - 4, 1);
        }

        // Subtle knot marks
        for (let k = 0; k < 2; k++) {
          const kx = x + 10 + Math.random() * (tileW - 20);
          const ky = Math.random() * tileH;
          ctx.fillStyle = 'rgba(0,0,0,0.08)';
          ctx.beginPath();
          ctx.ellipse(kx, ky, 6, 10, 0, 0, Math.PI * 2);
          ctx.fill();
        }
      }

      const map = new THREE.CanvasTexture(canvas);
      map.wrapS = THREE.RepeatWrapping;
      map.wrapT = THREE.RepeatWrapping;
      map.repeat.set(3, 1);

      return new THREE.MeshStandardMaterial({
        map,
        metalness: 0.05,
        roughness: 0.7,
        side: THREE.DoubleSide,
      });
    })(),

    deckRailing: new THREE.MeshStandardMaterial({
      color: 0x333333,
      metalness: 0.7,
      roughness: 0.3,
    }),

    railingGlass: new THREE.MeshPhysicalMaterial({
      color: 0xccddee,
      metalness: 0.0,
      roughness: 0.1,
      transparent: true,
      opacity: 0.15,
      side: THREE.DoubleSide,
    }),

    steppingStone: new THREE.MeshStandardMaterial({
      color: 0xccbb99,
      metalness: 0.0,
      roughness: 0.8,
    }),

    gardenSoil: new THREE.MeshStandardMaterial({
      color: 0x5a3d2b,
      metalness: 0.0,
      roughness: 1.0,
    }),

    gardenWall: new THREE.MeshStandardMaterial({
      color: 0x666666,
      metalness: 0.0,
      roughness: 0.9,
    }),

    lattice: new THREE.MeshStandardMaterial({
      color: 0xc4a36e,
      metalness: 0.0,
      roughness: 0.8,
    }),
  };
}
