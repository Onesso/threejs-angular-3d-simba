import * as THREE from 'three';

function createCorrugatedBumpMap(): THREE.CanvasTexture {
  const canvas = document.createElement('canvas');
  canvas.width = 256;
  canvas.height = 256;
  const ctx = canvas.getContext('2d')!;

  // Vertical corrugation pattern typical of shipping containers
  for (let x = 0; x < canvas.width; x++) {
    const brightness = Math.floor(128 + 127 * Math.sin((x / canvas.width) * Math.PI * 32));
    ctx.fillStyle = `rgb(${brightness},${brightness},${brightness})`;
    ctx.fillRect(x, 0, 1, canvas.height);
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
  const corrugatedBump = createCorrugatedBumpMap();

  return {
    containerBlue: (repeatX = 6) => {
      const bumpMap = corrugatedBump.clone();
      bumpMap.repeat.set(repeatX, 1);
      bumpMap.needsUpdate = true;
      return new THREE.MeshStandardMaterial({
        color: 0x1565c0,
        metalness: 0.5,
        roughness: 0.4,
        bumpMap,
        bumpScale: 0.03,
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

    deckWood: new THREE.MeshStandardMaterial({
      color: 0x8b6043,
      metalness: 0.0,
      roughness: 0.75,
      side: THREE.DoubleSide,
    }),

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
