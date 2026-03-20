import { Component } from '@angular/core';
import { SceneComponent } from './scene/scene.component';

@Component({
  selector: 'app-root',
  imports: [SceneComponent],
  templateUrl: './app.html',
  styleUrl: './app.scss',
})
export class App {}
